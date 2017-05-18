from collections import namedtuple
from ModestMaps.Core import Coordinate
from multiprocessing.pool import ThreadPool
from tilequeue.command import parse_layer_data
from tilequeue.format import extension_to_format
from tilequeue.format import json_format
from tilequeue.format import mvt_format
from tilequeue.format import topojson_format
from tilequeue.process import format_coord
from tilequeue.process import process_coord_no_format
from tilequeue.query import DataFetcher
from tilequeue.tile import coord_to_mercator_bounds
from tilequeue.utils import format_stacktrace_one_line
from tileserver.cache import CacheKey
from tileserver.cache import NullCache
from werkzeug.wrappers import Request
from werkzeug.wrappers import Response
import os.path
import psycopg2
import random
import yaml


def coord_is_valid(coord):
    if coord.zoom < 0 or coord.column < 0 or coord.row < 0:
        return False
    maxval = 2 ** coord.zoom
    if coord.column >= maxval or coord.row >= maxval:
        return False
    return True


RequestData = namedtuple('RequestData', 'layer_spec coord format tile_size')


def parse_request_path(
        path, extensions_to_handle, path_tile_size, max_interesting_zoom):
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
    if coord.zoom > max_interesting_zoom:
        return None
    request_data = RequestData(layer_spec, coord, format, tile_size)
    return request_data


LayerSpecParseResult = namedtuple(
    'LayerSpecParseResult',
    'layer_data unique_layer_names sorted_layer_names')


def parse_layer_spec(layer_spec, layer_config):
    """convert a layer spec into layer_data

    returns None if any specs in the optionally comma separated list
    are unknown layers"""
    if layer_spec == 'all':
        layer_data = layer_config.all_layers
        unique_layer_names = sorted_layer_names = ('all',)
    else:
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
    return LayerSpecParseResult(
        layer_data, unique_layer_names, sorted_layer_names)


def calculate_nominal_zoom(zoom, tile_size):
    assert tile_size >= 1
    return zoom + tile_size - 1


class TileServer(object):

    # whether to re-raise errors on request handling
    # we want this during development, but not during production
    propagate_errors = False

    def __init__(
            self, layer_config, extensions, data_fetcher, post_process_data,
            io_pool, store, cache, buffer_cfg, formats, health_checker=None,
            add_cors_headers=False, max_age=None, path_tile_size=None,
            max_interesting_zoom=None):
        self.layer_config = layer_config
        self.extensions = extensions
        self.data_fetcher = data_fetcher
        self.post_process_data = post_process_data
        self.io_pool = io_pool
        self.store = store
        self.cache = cache
        self.buffer_cfg = buffer_cfg
        self.formats = formats
        self.health_checker = health_checker
        self.add_cors_headers = add_cors_headers
        self.max_age = max_age
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
        headers = []
        if self.add_cors_headers:
            headers.append(('Access-Control-Allow-Origin', '*'))
        if self.max_age:
            headers.append(('Cache-Control', 'max-age=%d' % self.max_age))
        if headers:
            response_args['headers'] = headers
        response = Response(body, **response_args)

        if status == 200:
            response.add_etag()
            response.make_conditional(request)

        return response

    def handle_request(self, request):
        if (self.health_checker and
                self.health_checker.is_health_check(request)):
            return self.health_checker(request)

        request_data = parse_request_path(
            request.path, self.extensions, self.path_tile_size,
            self.max_interesting_zoom)
        if request_data is None:
            return self.generate_404(request)

        layer_spec = request_data.layer_spec
        layer_spec_result = parse_layer_spec(request_data.layer_spec,
                                             self.layer_config)
        if layer_spec_result is None:
            return self.generate_404(request)

        unique_layer_names = layer_spec_result.unique_layer_names
        sorted_layer_names = layer_spec_result.sorted_layer_names
        cache_key_layer_names = ','.join(sorted_layer_names)

        coord = request_data.coord
        format = request_data.format
        tile_size = request_data.tile_size

        cache_key = CacheKey(coord, tile_size, cache_key_layer_names, format)
        with self.cache.lock(cache_key):
            tile_data = self.cache.get(cache_key)

            if tile_data is not None:
                return self.create_response(
                    request, 200, tile_data, format.mimetype)

            nominal_zoom = calculate_nominal_zoom(coord.zoom, tile_size)

            cut_coords = ()
            # fetch data for all layers, even if the request was for a partial
            # set. this ensures that we can always store the result, allowing
            # for reuse, but also that any post-processing functions which
            # might have dependencies on multiple layers will still work
            # properly (e.g: buildings or roads layer being cut against
            # landuse).
            unpadded_bounds = coord_to_mercator_bounds(coord)
            feature_data_all = self.data_fetcher(
                nominal_zoom, unpadded_bounds, self.layer_config.all_layers)

            processed_feature_layers, extra_data = process_coord_no_format(
                feature_data_all['feature_layers'],
                nominal_zoom,
                unpadded_bounds,
                self.post_process_data,
            )

            if layer_spec != 'all':
                kept_feature_layers = []
                for feature_layer in processed_feature_layers:
                    name = feature_layer['layer_datum']['name']
                    if name in unique_layer_names:
                        kept_feature_layers.append(feature_layer)
                processed_feature_layers = kept_feature_layers

            formatted_tiles, extra_data = format_coord(
                coord,
                nominal_zoom,
                processed_feature_layers,
                (format,),
                unpadded_bounds,
                cut_coords,
                self.buffer_cfg,
                extra_data,
            )

            assert len(formatted_tiles) == 1
            tile_data = formatted_tiles[0]['tile']

            self.cache.set(cache_key, tile_data)

        response = self.create_response(
            request, 200, tile_data, format.mimetype)
        return response


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

    cache = NullCache()
    cache_config = config.get('cache')
    if cache_config:
        cache_type = cache_config.get('type')
        if cache_type == 'redis':
            import redis
            from tileserver.cache import RedisCache
            redis_config = cache_config.get('redis', {})
            redis_client = redis.from_url(redis_config.get('url'))
            redis_options = redis_config.get('options') or {}
            cache = RedisCache(redis_client, **redis_options)
        elif cache_type == 'file':
            from tileserver.cache import FileCache
            file_config = cache_config.get('file', {})
            cache = FileCache(file_config.get('prefix'))

    health_checker = None
    health_check_config = config.get('health')
    if health_check_config:
        health_check_url = health_check_config['url']
        health_checker = HealthChecker(health_check_url, conn_info)

    http_cfg = config.get('http', {})
    add_cors_headers = bool(http_cfg.get('cors', False))
    max_age = http_cfg.get('max-age')
    if max_age is not None:
        max_age = int(max_age)

    path_tile_size = config.get('path_tile_size')
    max_interesting_zoom = config.get('max_interesting_zoom')

    tile_server = TileServer(
        layer_config, extensions, data_fetcher, post_process_data, io_pool,
        store, cache, buffer_cfg, formats, health_checker, add_cors_headers,
        max_age, path_tile_size, max_interesting_zoom)
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
