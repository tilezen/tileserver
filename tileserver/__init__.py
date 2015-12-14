from collections import namedtuple
from cStringIO import StringIO
from ModestMaps.Core import Coordinate
from multiprocessing.pool import ThreadPool
from tilequeue.command import parse_layer_data
from tilequeue.format import extension_to_format
from tilequeue.format import json_format
from tilequeue.process import process_coord
from tilequeue.query import DataFetcher
from tilequeue.tile import coord_to_mercator_bounds
from tilequeue.tile import pad_bounds_for_zoom
from tilequeue.tile import reproject_lnglat_to_mercator
from tilequeue.tile import serialize_coord
from tilequeue.transform import mercator_point_to_wgs84
from tilequeue.transform import transform_feature_layers_shape
from tilequeue.utils import format_stacktrace_one_line
from werkzeug.wrappers import Request
from werkzeug.wrappers import Response
import json
import psycopg2
import random
import shapely.geometry
import shapely.ops
import shapely.wkb
import yaml


def coord_is_valid(coord):
    if coord.zoom < 0 or coord.column < 0 or coord.row < 0:
        return False
    maxval = 2 ** coord.zoom
    if coord.column >= maxval or coord.row >= maxval:
        return False
    return True


RequestData = namedtuple('RequestData', 'layer_spec coord format')


def parse_request_path(path):
    """given a path, parse the underlying layer, coordinate, and format"""
    parts = path.split('/')
    if len(parts) != 5:
        return None
    _, layer_spec, zoom_str, column_str, row_and_ext = parts
    row_fields = row_and_ext.split('.')
    if len(row_fields) != 2:
        return None
    row_str, ext = row_fields
    format = extension_to_format.get(ext)
    if format is None:
        return None
    try:
        zoom = int(zoom_str)
        column = int(column_str)
        row = int(row_str)
    except ValueError:
        return None
    coord = Coordinate(zoom=zoom, column=column, row=row)
    if not coord_is_valid(coord):
        return None
    request_data = RequestData(layer_spec, coord, format)
    return request_data


def parse_layer_spec(layer_spec, layer_config):
    """convert a layer spec into layer_data

    returns None is any specs in the optionally comma separated list
    are unknown layers"""
    if layer_spec == 'all':
        return layer_config.all_layers
    individual_layer_names = layer_spec.split(',')
    unique_layer_names = set()
    for layer_name in individual_layer_names:
        if layer_name == 'all':
            if 'all' not in unique_layer_names:
                for all_layer_datum in layer_config.all_layers:
                    unique_layer_names.add(all_layer_datum['name'])
        unique_layer_names.add(layer_name)
    sorted_layer_names = sorted(unique_layer_names)
    layer_data = []
    for layer_name in sorted_layer_names:
        if layer_name == 'all':
            continue
        layer_datum = layer_config.layer_data_by_name.get(layer_name)
        if layer_datum is None:
            return None
        layer_data.append(layer_datum)
    return layer_data


def ensure_utf8_properties(props):
    new_props = {}
    for k, v in props.items():
        if isinstance(k, unicode):
            k = k.encode('utf-8')
        if isinstance(v, unicode):
            v = v.encode('utf-8')
        new_props[k] = v
    return new_props


def decode_json_tile_for_layers(tile_data, layer_data):
    layer_names_to_keep = set(ld['name'] for ld in layer_data)
    feature_layers = []
    json_data = json.loads(tile_data)
    for layer_name, json_layer_data in json_data.items():
        if layer_name not in layer_names_to_keep:
            continue
        features = []
        json_features = json_layer_data['features']
        for json_feature in json_features:
            json_geometry = json_feature['geometry']
            shape_lnglat = shapely.geometry.shape(json_geometry)
            shape_mercator = shapely.ops.transform(
                reproject_lnglat_to_mercator, shape_lnglat)
            properties = json_feature['properties']
            # Ensure that we have strings for all key values and not
            # unicode values. Some of the encoders except to be
            # working with strings directly
            properties = ensure_utf8_properties(properties)
            fid = None
            feature = shape_mercator, properties, fid
            features.append(feature)
        # a further transform asks for a layer_datum is_clipped
        # property where it applies clipping
        # but this data coming from json is already clipped
        feature_layer = dict(
            name=layer_name,
            features=features,
            layer_datum=dict(is_clipped=False),
        )
        feature_layers.append(feature_layer)
    return feature_layers


def reformat_selected_layers(json_tile_data, layer_data, coord, format):
    """
    Reformats the selected (subset of) layers from a JSON tile containing all
    layers. We store "tiles of record" containing all layers as JSON, and this
    function does most of the work of reading that, pruning the layers which
    aren't needed and reformatting it to the desired output format.
    """

    feature_layers = decode_json_tile_for_layers(tile_data, layer_data)
    bounds_merc = coord_to_mercator_bounds(coord)
    bounds_wgs84 = (
        mercator_point_to_wgs84(bounds_merc[:2]) +
        mercator_point_to_wgs84(bounds_merc[2:4]))
    padded_bounds_merc = pad_bounds_for_zoom(bounds_merc, coord.zoom)

    scale = 4096
    feature_layers = transform_feature_layers_shape(
        feature_layers, format, scale, bounds_merc,
        padded_bounds_merc, coord)

    tile_data_file = StringIO()
    format.format_tile(tile_data_file, feature_layers, coord,
                       bounds_merc, bounds_wgs84)
    tile_data = tile_data_file.getvalue()
    return tile_data


class TileServer(object):

    # whether to re-raise errors on request handling
    # we want this during development, but not during production
    propagate_errors = False

    def __init__(self, layer_config, data_fetcher, post_process_data,
                 io_pool, store, redis_cache_index, health_checker=None):
        self.layer_config = layer_config
        self.data_fetcher = data_fetcher
        self.post_process_data = post_process_data
        self.io_pool = io_pool
        self.store = store
        self.redis_cache_index = redis_cache_index
        self.health_checker = health_checker

    def __call__(self, environ, start_response):
        request = Request(environ)
        try:
            response = self.handle_request(request)
        except:
            if self.propagate_errors:
                raise
            stacktrace = format_stacktrace_one_line()
            print 'Error handling request for %s: %s' % (
                request.path, stacktrace)
            response = Response(
                'Internal Server Error', status=500, mimetype='text/plain')
        return response(environ, start_response)

    def generate_404(self):
        return Response('Not Found', status=404, mimetype='text/plain')

    def create_response(self, request, tile_data, format):
        response = Response(
            tile_data,
            mimetype=format.mimetype,
            headers=[('Access-Control-Allow-Origin', '*')])
        response.add_etag()
        response.make_conditional(request)
        return response

    def handle_request(self, request):
        if (self.health_checker and
                self.health_checker.is_health_check(request)):
            return self.health_checker(request)
        request_data = parse_request_path(request.path)
        if request_data is None:
            return self.generate_404()
        layer_spec = request_data.layer_spec
        layer_data = parse_layer_spec(request_data.layer_spec,
                                      self.layer_config)
        if layer_data is None:
            return self.generate_404()

        coord = request_data.coord
        format = request_data.format

        if self.store and coord.zoom <= 20:
            # we have a dynamic layer request
            # in this case, we should try to fetch the data from the
            # cache, and if present, prune the layers that aren't
            # necessary from there.
            tile_data = self.store.read_tile(coord, json_format)
            if tile_data is not None:
                tile_data = reformat_selected_layers(tile_data, layer_data,
                        coord, format)
                return self.create_response(request, tile_data, format)

        # fetch data for all layers, even if the request was for a partial set.
        # this ensures that we can always store the result, allowing for reuse,
        # but also that any post-processing functions which might have
        # dependencies on multiple layers will still work properly (e.g:
        # buildings or roads layer being cut against landuse).
        feature_data_all = self.data_fetcher(coord, self.layer_config.all_layers)

        formatted_tiles_all = process_coord(
            coord,
            feature_data_all['feature_layers'],
            self.post_process_data,
            [json_format],
            feature_data_all['unpadded_bounds'],
            feature_data_all['padded_bounds'],
            [])
        assert len(formatted_tiles_all) == 1, \
            'unexpected number of tiles: %d' % len(formatted_tiles_all)
        formatted_tile_all = formatted_tiles_all[0]
        tile_data_all = formatted_tile_all['tile']

        # store tile with data for all layers to the cache, so that we can read
        # it all back for the dynamic layer request above.
        if self.store and coord.zoom <= 20:
            self.io_pool.apply_async(
                async_store, (self.store, tile_data_all, coord, json_format))

        # update the tiles of interest set with the new coordinate
        if self.redis_cache_index:
            self.io_pool.apply_async(async_update_tiles_of_interest,
                                     (self.redis_cache_index, coord))

        if layer_spec == 'all':
            if format == json_format:
                # already done all the work, just need to return the tile to
                # the client.
                tile_data = tile_data_all

            else:
                # just need to format the data differently
                tile_data = reformat_selected_layers(
                    tile_data_all, self.layer_config.all_layers, coord, format)

                # note that we want to store the data too, as this means that
                # future requests can be serviced directly from the store.
                if self.store and coord.zoom <= 20:
                    self.io_pool.apply_async(
                        async_store, (self.store, tile_data, coord, format))

        else:
            # select the data that the user actually asked for from the JSON/all
            # tile that we just created.
            tile_data = reformat_selected_layers(
                tile_data_all, layer_data, coord, format)

        response = self.create_response(request, tile_data, format)
        return response


def async_store(store, tile_data, coord, format):
    """update cache store with tile_data"""
    try:
        store.write_tile(tile_data, coord, format)
    except:
        stacktrace = format_stacktrace_one_line()
        print 'Error storing coord %s with format %s: %s' % (
            serialize_coord(coord), format.extension, stacktrace)


def async_update_tiles_of_interest(redis_cache_index, coord):
    """update tiles of interest set

    The tiles of interest represent all tiles that will get processed
    on osm diffs. Our policy is to cache tiles up to zoom level 20. As
    an optimization, because the queries only change up until zoom
    level 18, ie they are the same for z18+, we enqueue work at z18,
    and the z19 and z20 tiles get generated by cutting the z18 tile
    appropriately. This means that when we receive requests for tiles
    > z18, we need to also track the corresponding tile at z18,
    otherwise those tiles would never get regenerated.
    """
    try:
        if coord.zoom <= 20:
            redis_cache_index.index_coord(coord)
        if coord.zoom > 18:
            coord_at_z18 = coord.zoomTo(18).container()
            redis_cache_index.index_coord(coord_at_z18)
    except:
        stacktrace = format_stacktrace_one_line()
        print 'Error updating tiles of interest for coord %s: %s\n' % (
            serialize_coord(coord), stacktrace)


class LayerConfig(object):

    def __init__(self, all_layer_names, layer_data):
        self.all_layer_names = sorted(all_layer_names)
        self.layer_data = layer_data
        self.layer_data_by_name = dict(
            (layer_datum['name'], layer_datum) for layer_datum in layer_data)
        self.all_layers = [self.layer_data_by_name[x]
                           for x in self.all_layer_names]


def make_store(store_type, store_name, store_config):
    if store_type == 'directory':
        from tilequeue.store import make_tile_file_store
        return make_tile_file_store(store_name)

    elif store_type == 's3':
        from tilequeue.store import make_s3_store
        path = store_config.get('path', 'osm')
        date_prefix = store_config.get('date-prefix', '')
        reduced_redundancy = store_config.get('reduced_redundancy', True)
        return make_s3_store(
            store_name, path=path, reduced_redundancy=reduced_redundancy,
            date_prefix=date_prefix)

    else:
        raise ValueError('Unrecognized store type: `{}`'.format(store_type))


class HealthChecker(object):

    def __init__(self, url, conn_info):
        self.url = url
        conn_info_dbnames = conn_info.copy()
        self.dbnames = conn_info_dbnames.pop('dbnames')
        assert len(self.dbnames) > 0
        self.conn_info_no_dbname = conn_info_dbnames

    def is_health_check(self, request):
        return request.path == self.url

    def __call__(self, request):
        dbname = random.choice(self.dbnames)
        conn_info = dict(self.conn_info_no_dbname, dbname=dbname)
        conn = psycopg2.connect(**conn_info)
        conn.set_session(readonly=True, autocommit=True)
        try:
            cursor = conn.cursor()
            cursor.execute('select 1')
            records = cursor.fetchall()
            assert len(records) == 1
            assert len(records[0]) == 1
            assert records[0][0] == 1
        finally:
            conn.close()
        return Response('OK', mimetype='text/plain')


def create_tileserver_from_config(config):
    """create a tileserve object from yaml configuration"""
    query_config = config['queries']
    queries_config_path = query_config['config']
    template_path = query_config['template-path']
    reload_templates = query_config['reload-templates']

    with open(queries_config_path) as query_cfg_fp:
        queries_config = yaml.load(query_cfg_fp)
    all_layer_data, layer_data, post_process_data = parse_layer_data(
        queries_config, template_path, reload_templates)
    all_layer_names = [x['name'] for x in all_layer_data]
    layer_config = LayerConfig(all_layer_names, layer_data)

    conn_info = config['postgresql']
    n_conn = len(layer_data)
    io_pool = ThreadPool(n_conn)
    data_fetcher = DataFetcher(
        conn_info, all_layer_data, io_pool, n_conn)

    store = None
    store_config = config.get('store')
    if store_config:
        store_type = store_config.get('type')
        store_name = store_config.get('name')
        if store_type and store_name:
            store = make_store(store_type, store_name, store_config)

    redis_cache_index = None
    redis_config = config.get('redis')
    if redis_config:
        from redis import StrictRedis
        from tilequeue.cache import RedisCacheIndex
        redis_host = redis_config.get('host', 'localhost')
        redis_port = redis_config.get('port', 6379)
        redis_db = redis_config.get('db', 0)
        redis_client = StrictRedis(redis_host, redis_port, redis_db)
        redis_cache_index = RedisCacheIndex(redis_client)

    health_checker = None
    health_check_config = config.get('health')
    if health_check_config:
        health_check_url = health_check_config['url']
        health_checker = HealthChecker(health_check_url, conn_info)

    tile_server = TileServer(
        layer_config, data_fetcher, post_process_data, io_pool, store,
        redis_cache_index, health_checker)
    return tile_server


def wsgi_server(config_path):
    """create wsgi server given a config path"""
    with open(config_path) as fp:
        config = yaml.load(fp)
    tile_server = create_tileserver_from_config(config)
    return tile_server


if __name__ == '__main__':
    from werkzeug.serving import run_simple
    import sys

    if len(sys.argv) == 1:
        print 'Pass in path to config file'
        sys.exit(1)

    config_path = sys.argv[1]
    with open(config_path) as fp:
        config = yaml.load(fp)

    tile_server = create_tileserver_from_config(config)
    tile_server.propagate_errors = True

    server_config = config['server']
    run_simple(server_config['host'], server_config['port'], tile_server,
               threaded=server_config.get('threaded', False),
               use_debugger=server_config.get('debug', False),
               use_reloader=server_config.get('reload', False))
