from collections import namedtuple
from ModestMaps.Core import Coordinate
from multiprocessing.pool import ThreadPool
from tilequeue.command import parse_layer_data_layers
from tilequeue.format import extension_to_format
from tilequeue.process import process_coord
from tilequeue.query import DataFetcher
from tilequeue.tile import serialize_coord
from tilequeue.utils import format_stacktrace_one_line
from TileStache import parseConfigfile
from werkzeug.wrappers import Request
from werkzeug.wrappers import Response
import yaml


def coord_is_valid(coord):
    if coord.zoom < 0 or coord.column < 0 or coord.row < 0:
        return False
    if coord.zoom > 20:
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


class TileServer(object):

    def __init__(self, layer_config, data_fetcher, store, io_pool):
        self.layer_config = layer_config
        self.data_fetcher = data_fetcher
        self.store = store
        self.io_pool = io_pool

    def __call__(self, environ, start_response):
        request = Request(environ)
        response = self.handle_request(request)
        return response(environ, start_response)

    def generate_404(self):
        return Response('Not Found', 404, mimetype='text/plain')

    def handle_request(self, request):
        request_data = parse_request_path(request.path)
        if request_data is None:
            return self.generate_404()
        layer_data = parse_layer_spec(request_data.layer_spec,
                                      self.layer_config)
        if layer_data is None:
            return self.generate_404()

        coord = request_data.coord
        format = request_data.format
        feature_data = self.data_fetcher(coord, layer_data)
        formatted_tiles = process_coord(
            coord,
            feature_data['feature_layers'],
            [format],
            feature_data['unpadded_bounds'],
            feature_data['padded_bounds'],
            [])
        assert len(formatted_tiles) == 1, \
            'unexpected number of tiles: %d' % len(formatted_tiles)
        formatted_tile = formatted_tiles[0]
        tile_data = formatted_tile['tile']

        if self.store:
            self.io_pool.apply_async(
                async_store, (self.store, tile_data, coord, format))

        response = Response(tile_data, mimetype=format.mimetype)
        response.add_etag()
        response.make_conditional(request)
        return response


def async_store(store, tile_data, coord, format):
    """store tile_data in the background"""
    try:
        store.write_tile(tile_data, coord, format)
    except:
        stacktrace = format_stacktrace_one_line()
        print 'Error storing coord %s with format %s: %s' % (
            serialize_coord(coord), format.extension, stacktrace)


class LayerConfig(object):

    def __init__(self, all_layer_names, layer_data):
        self.all_layer_names = sorted(all_layer_names)
        self.layer_data = layer_data
        self.layer_data_by_name = dict(
            (layer_datum['name'], layer_datum) for layer_datum in layer_data)
        self.all_layers = [self.layer_data_by_name[x]
                           for x in self.all_layer_names]


def parse_tilestache_config(tilestache_config):
    """generate layer config from tilestache config object

    don't just take the all layer, because some layers exist that
    aren't a part of all"""
    layers = tilestache_config.layers
    all_layer = layers.get('all')
    assert all_layer is not None, 'all layer is expected in tilestache config'
    all_layer_names = all_layer.provider.names
    layer_names = [x for x in layers if x not in ('all', ',')]
    layer_data = parse_layer_data_layers(tilestache_config, layer_names)
    layer_config = LayerConfig(all_layer_names, layer_data)
    return layer_config


def make_store(store_type, store_name, store_config):
    if store_type == 'directory':
        from tilequeue.store import make_tile_file_store
        return make_tile_file_store(store_name)

    elif store_type == 's3':
        from tilequeue.store import make_s3_store
        path = store_config.get('path', 'osm')
        reduced_redundancy = store_config.get('reduced_redundancy', True)
        return make_s3_store(
            store_name, path=path, reduced_redundancy=reduced_redundancy)

    else:
        raise ValueError('Unrecognized store type: `{}`'.format(store_type))


def create_tileserver_from_config(config):
    """create a tileserve object from yaml configuration"""
    tilestache_config_path = config['tilestache']['config']
    tilestache_config = parseConfigfile(tilestache_config_path)
    layer_config = parse_tilestache_config(tilestache_config)

    conn_info = config['postgresql']
    n_conn = len(layer_config.layer_data)
    io_pool = ThreadPool(n_conn)
    data_fetcher = DataFetcher(
        conn_info, layer_config.all_layers, io_pool, n_conn)

    store_config = config['store']
    store = make_store(
        store_config['type'], store_config['name'], store_config)

    tile_server = TileServer(layer_config, data_fetcher, store, io_pool)
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

    server_config = config['server']
    run_simple(server_config['host'], server_config['port'], tile_server,
               use_debugger=server_config.get('debug', False),
               use_reloader=server_config.get('reload', False))
