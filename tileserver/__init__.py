from collections import namedtuple
from cStringIO import StringIO
from ModestMaps.Core import Coordinate
from multiprocessing.pool import ThreadPool
from tilequeue.command import parse_layer_data
from tilequeue.format import extension_to_format
from tilequeue.format import json_format, zip_format, topojson_format, \
    mvt_format
from tilequeue.process import process_coord
from tilequeue.query import DataFetcher
from tilequeue.tile import calc_meters_per_pixel_dim
from tilequeue.tile import coord_children_range
from tilequeue.tile import coord_to_mercator_bounds
from tilequeue.tile import reproject_lnglat_to_mercator
from tilequeue.tile import serialize_coord
from tilequeue.transform import mercator_point_to_lnglat
from tilequeue.transform import transform_feature_layers_shape
from tilequeue.utils import format_stacktrace_one_line
from tilequeue.metatile import extract_metatile
from werkzeug.wrappers import Request
from werkzeug.wrappers import Response
import ujson as json
import psycopg2
import random
import shapely.geometry
import shapely.ops
import shapely.wkb
import yaml
import os.path
import math


def coord_is_valid(coord):
    if coord.zoom < 0 or coord.column < 0 or coord.row < 0:
        return False
    maxval = 2 ** coord.zoom
    if coord.column >= maxval or coord.row >= maxval:
        return False
    return True


RequestData = namedtuple('RequestData', 'layer_spec coord format tile_size')


def parse_request_path(path, extensions_to_handle, path_tile_size=None):
    """given a path, parse the underlying layer, coordinate, and format"""
    parts = path.split('/')

    # allow an optional prefix at the beginning of the URL, which would
    # normally be "/layers/z/x/y.fmt". this means we take the name from
    # "/prefix/layers/z/x/y.fmt", if the path is formatted that way.
    tile_size = 1
    if len(parts) == 6 and path_tile_size is not None:
        prefix = parts.pop(1)
        # look up the prefix in the argument "path_tile_size", which is
        # expected to be a dict mapping prefix to tile size (integer).
        tile_size = path_tile_size.get(prefix)
        if tile_size is None:
            return None

    if len(parts) != 5:
        return None
    _, layer_spec, zoom_str, column_str, row_and_ext = parts
    row_fields = row_and_ext.split('.')
    if len(row_fields) != 2:
        return None
    row_str, ext = row_fields
    if ext not in extensions_to_handle:
        return None
    format = extension_to_format.get(ext)
    assert format, 'Unknown extension %s' % ext
    try:
        zoom = int(zoom_str)
        column = int(column_str)
        row = int(row_str)
    except ValueError:
        return None
    coord = Coordinate(zoom=zoom, column=column, row=row)
    if not coord_is_valid(coord):
        return None
    request_data = RequestData(layer_spec, coord, format, tile_size)
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


def reformat_selected_layers(
        json_tile_data, layer_data, coord, format, buffer_cfg):
    """
    Reformats the selected (subset of) layers from a JSON tile containing all
    layers. We store "tiles of record" containing all layers as JSON, and this
    function does most of the work of reading that, pruning the layers which
    aren't needed and reformatting it to the desired output format.
    """

    feature_layers = decode_json_tile_for_layers(json_tile_data, layer_data)
    bounds_merc = coord_to_mercator_bounds(coord)
    bounds_lnglat = (
        mercator_point_to_lnglat(bounds_merc[0], bounds_merc[1]) +
        mercator_point_to_lnglat(bounds_merc[2], bounds_merc[3]))

    meters_per_pixel_dim = calc_meters_per_pixel_dim(coord.zoom)

    scale = 4096
    feature_layers = transform_feature_layers_shape(
        feature_layers, format, scale, bounds_merc,
        meters_per_pixel_dim, buffer_cfg)

    tile_data_file = StringIO()
    format.format_tile(tile_data_file, feature_layers, coord.zoom,
                       bounds_merc, bounds_lnglat)
    tile_data = tile_data_file.getvalue()
    return tile_data


class TileServer(object):

    # whether to re-raise errors on request handling
    # we want this during development, but not during production
    propagate_errors = False

    def __init__(self, layer_config, extensions, data_fetcher,
                 post_process_data, io_pool, store,
                 buffer_cfg, formats, health_checker=None,
                 add_cors_headers=False, metatile_size=None,
                 metatile_store_originals=False, path_tile_size=None,
                 max_interesting_zoom=None, cache=False):
        self.layer_config = layer_config
        self.extensions = extensions
        self.data_fetcher = data_fetcher
        self.post_process_data = post_process_data
        self.io_pool = io_pool
        self.store = store
        self.buffer_cfg = buffer_cfg
        self.formats = formats
        self.health_checker = health_checker
        self.add_cors_headers = add_cors_headers
        self.metatile_size = metatile_size
        if self.metatile_size is not None:
            self.metatile_zoom = int(math.log(self.metatile_size, 2))
            assert self.metatile_size == (1 << self.metatile_zoom), \
                "Metatile sizes must be a power of two, but %d doesn't look " \
                "like one." % self.metatile_size
        self.metatile_store_originals = metatile_store_originals
        self.path_tile_size = path_tile_size or {}
        self.max_interesting_zoom = max_interesting_zoom or 20

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
            response = self.create_response(
                request, 500, 'Internal Server Error', 'text/plain')
        return response(environ, start_response)

    def generate_404(self, request):
        return self.create_response(request, 404, 'Not Found', 'text/plain')

    def create_response(self, request, status, body, mimetype):
        response_args = dict(
            status=status,
            mimetype=mimetype,
        )
        if self.add_cors_headers:
            response_args['headers'] = [('Access-Control-Allow-Origin', '*')]
        response = Response(body, **response_args)

        if status == 200:
            response.add_etag()
            response.make_conditional(request)

        return response

    def handle_request(self, request):
        if (self.health_checker and
                self.health_checker.is_health_check(request)):
            return self.health_checker(request)

        request_data = parse_request_path(request.path, self.extensions,
                                          self.path_tile_size)
        if request_data is None:
            return self.generate_404(request)

        layer_spec = request_data.layer_spec
        layer_data = parse_layer_spec(request_data.layer_spec,
                                      self.layer_config)
        if layer_data is None:
            return self.generate_404(request)

        coord = request_data.coord
        format = request_data.format

        with self.cache.lock(coord):
            tile_data = self.cache.get(coord)

            if tile_data is None:
                tile_data = self.reformat_from_stored_json(
                    request_data, layer_data)

            if tile_data is not None:
                return self.create_response(
                    request, 200, tile_data, format.mimetype)

            tile_size = request_data.tile_size
            meta_coord, offset = self.coord_split(coord, tile_size)

            if self.using_metatiles():
                # make all formats when making metatiles
                wanted_formats = self.formats

            else:
                wanted_formats = [json_format]
                # add the request format, so that it gets created by the tile
                # render process and will be saved along with the JSON format.
                if format != json_format:
                    wanted_formats.append(format)

            nominal_zoom = coord.zoom
            cut_coords = []
            if self.using_metatiles():
                nominal_zoom = meta_coord.zoom + self.metatile_zoom
                if self.metatile_zoom > 0:
                    cut_coords.extend(
                        coord_children_range(meta_coord, nominal_zoom))

            # fetch data for all layers, even if the request was for a partial
            # set. this ensures that we can always store the result, allowing
            # for reuse, but also that any post-processing functions which
            # might have dependencies on multiple layers will still work
            # properly (e.g: buildings or roads layer being cut against
            # landuse).
            unpadded_bounds = coord_to_mercator_bounds(meta_coord)
            feature_data_all = self.data_fetcher(
                nominal_zoom, unpadded_bounds, self.layer_config.all_layers)

            formatted_tiles_all, extra_data = process_coord(
                meta_coord,
                nominal_zoom,
                feature_data_all['feature_layers'],
                self.post_process_data,
                wanted_formats,
                feature_data_all['unpadded_bounds'],
                cut_coords, self.buffer_cfg)

            expected_tile_count = len(wanted_formats) * (1 + len(cut_coords))
            assert len(formatted_tiles_all) == expected_tile_count, \
                'unexpected number of tiles: %d, wanted %d' \
                % (len(formatted_tiles_all), expected_tile_count)

            if layer_spec == 'all':
                tile_data = self.extract_tile_data(
                    coord, format, formatted_tiles_all)

            else:
                # select the data that the user actually asked for from the
                # JSON/all tile that we just created.
                json_data_all = self.extract_tile_data(
                    coord, json_format, formatted_tiles_all)

                tile_data = reformat_selected_layers(
                    json_data_all, layer_data, coord, format, self.buffer_cfg)

            self.cache.set(coord, tile_data)

        response = self.create_response(
            request, 200, tile_data, format.mimetype)
        return response

    def reformat_from_stored_json(self, request_data, layer_data):
        layer_spec = request_data.layer_spec
        coord = request_data.coord
        format = request_data.format

        if not self.store or coord.zoom > 20:
            return None

        tile_size = request_data.tile_size
        meta_coord, offset = self.coord_split(coord, tile_size)

        # we either have a dynamic layer request, or it's a request for a new
        # tile that is not currently in the tiles of interest, or it's for a
        # request that's in the tiles of interest that hasn't been generated,
        # possibly because a new prefix is used and all tiles haven't been
        # generated yet before making the switch

        # in any case, it makes sense to try and fetch the json format from
        # the store first
        tile_data = self.read_tile(meta_coord, offset)
        if tile_data is None:
            return None

        # the json format exists in the store we'll use it to generate the
        # response. don't need to reformat if the tile is already in JSON.
        if layer_spec != 'all' or format != json_format:
            tile_data = reformat_selected_layers(
                tile_data, layer_data, coord, format, self.buffer_cfg)

        return tile_data

    def extract_tile_data(self, coord, fmt, formatted_tiles_all):
        for tile in formatted_tiles_all:
            if tile['format'] == fmt and tile['coord'] == coord:
                return tile['tile']

        raise KeyError("Unable to find format %r at coordinate %r in "
                       "formatted tiles." % (fmt, coord))

    def read_tile(self, coord, offset=None):
        if self.using_metatiles():
            fmt = zip_format
        else:
            fmt = json_format

        raw_data = None
        try:
            raw_data = self.store.read_tile(coord, fmt, 'all')
        except:
            stacktrace = format_stacktrace_one_line()
            print 'Error reading coord %s with format %s: %s' % (
                serialize_coord(coord), format.extension, stacktrace)

        if raw_data is None:
            return None

        if self.using_metatiles():
            zip_io = StringIO(raw_data)
            return extract_metatile(zip_io, json_format, offset)

        else:
            return raw_data

    def using_metatiles(self):
        return self.metatile_size is not None

    def coord_split(self, request_coord, request_tile_size):
        """
        A metatile can store many coordinates, which means that the coordinate
        of the metatile might be different from the coordinate of the tile
        which was requested. This method splits the tile coordinate into two
        parts; the metatile part and the "offset" within the metatile.
        """

        if not self.using_metatiles():
            return request_coord, None

        assert request_tile_size <= self.metatile_size, \
            "Request for tile with size greater than the metatile size."

        metatile_zoom = int(math.log(self.metatile_size, 2))
        tile_size_zoom = int(math.log(request_tile_size, 2))
        delta_zoom = metatile_zoom - tile_size_zoom

        # if the metatile would have a zoom of less than zero, then clamp to
        # zero. this means returning a tile at the wrong nominal zoom, but
        # that might be preferable to not returning a tile at all.
        if request_coord.zoom < delta_zoom:
            meta_coord = Coordinate(0, 0, 0)
            offset_coord = Coordinate(0, 0, 0)

        else:
            meta_coord = Coordinate(
                zoom=request_coord.zoom - delta_zoom,
                column=request_coord.column >> delta_zoom,
                row=request_coord.row >> delta_zoom)
            offset_coord = Coordinate(
                zoom=delta_zoom,
                column=(request_coord.column -
                        (meta_coord.column << delta_zoom)),
                row=request_coord.row - (meta_coord.row << delta_zoom))

        return meta_coord, offset_coord


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
    buffer_cfg = config.get('buffer', {})

    extensions_config = config.get('formats')
    extensions = set()
    formats = []
    if extensions_config:
        for extension in extensions_config:
            assert extension in extension_to_format, \
                'Unknown format: %s' % extension
            extensions.add(extension)
            formats.append(extension_to_format[extension])
    else:
        extensions = set(['json', 'topojson', 'mvt'])
        formats = [json_format, topojson_format, mvt_format]

    with open(queries_config_path) as query_cfg_fp:
        queries_config = yaml.load(query_cfg_fp)
    all_layer_data, layer_data, post_process_data = parse_layer_data(
        queries_config, buffer_cfg, template_path, reload_templates,
        os.path.dirname(queries_config_path))
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

    health_checker = None
    health_check_config = config.get('health')
    if health_check_config:
        health_check_url = health_check_config['url']
        health_checker = HealthChecker(health_check_url, conn_info)

    add_cors_headers = config.get('cors', False)

    metatile_size = None
    metatile_store_originals = False
    metatile_config = config.get('metatile')
    if metatile_config:
        metatile_size = metatile_config.get('size')
        metatile_store_originals = metatile_config.get(
            'store_metatile_and_originals')
    path_tile_size = config.get('path_tile_size')
    max_interesting_zoom = config.get('max_interesting_zoom')

    tile_server = TileServer(
        layer_config, extensions, data_fetcher, post_process_data, io_pool,
        store, buffer_cfg, formats, health_checker, add_cors_headers,
        metatile_size, metatile_store_originals, path_tile_size,
        max_interesting_zoom)
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
