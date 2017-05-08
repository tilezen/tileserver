import unittest


class CacheHelperTests(unittest.TestCase):
    def test_clean_empty_parent_dirs(self):
        import os
        from tileserver.cache import clean_empty_parent_dirs, mkdir_p

        mkdir_p('foo/bar/baz')
        with open('foo/bar/baz/hello.txt', 'w') as f:
            f.write('hello world')
        clean_empty_parent_dirs('foo/bar/baz/hello.txt')
        self.assertTrue(
            os.path.exists('foo/bar/baz/hello.txt'),
            "The directory is not empty, so it should not have been deleted")
        os.remove('foo/bar/baz/hello.txt')

        clean_empty_parent_dirs('foo/bar/baz', 'foo/bar')
        self.assertTrue(
            os.path.exists('foo/bar'),
            "Shouldn't have deleted the parent_dir")

        clean_empty_parent_dirs('foo/bar')
        self.assertFalse(
            os.path.exists('foo'),
            "Should have deleted everything")


class FileCacheTests(unittest.TestCase):
    def test_obtain_lock(self):
        from ModestMaps.Core import Coordinate
        from tileserver.cache import FileCache

        coord = Coordinate(0, 0, 0)

        c = FileCache('foo')
        try:
            c.obtain_lock(coord)
        finally:
            c.release_lock(coord)

    def test_obtain_lock_already_locked(self):
        from ModestMaps.Core import Coordinate
        from tileserver.cache import FileCache, LockTimeout

        coord = Coordinate(0, 0, 0)

        c = FileCache('foo')
        try:
            # Obtain a lock from "client A"
            c.obtain_lock(coord)
            with self.assertRaises(LockTimeout):
                # Locking from "client B" should time out
                c.obtain_lock(coord, timeout=1)
        finally:
            c.release_lock(coord)

        # After releasing, obtaining the lock from "client A" should work
        c.obtain_lock(coord)
        c.release_lock(coord)

    def test_set_get(self):
        import os
        from ModestMaps.Core import Coordinate
        from tileserver.cache import FileCache, clean_empty_parent_dirs
        coord = Coordinate(0, 0, 0)
        tile_data = 'hello world'

        c = FileCache('foo')
        c.set(coord, tile_data)
        actual_data = c.get(coord)

        self.assertEquals(tile_data, actual_data)

        os.remove('foo/00/000/000/000/000/000/000.data')
        clean_empty_parent_dirs('foo/00/000/000/000/000/000')


class MockRedis(object):
    def __init__(self):
        self._data = {}

    def set(self, key, data):
        self._data[key] = data

    def get(self, key):
        return self._data.get(key)

    def delete(self, key):
        del self._data[key]

    def setnx(self, key, data):
        if key in self._data:
            return False
        else:
            self.set(key, data)

    def getset(self, key, data):
        val = self._data.get(key)
        self._data[key] = data
        return val


class RedisCacheTests(unittest.TestCase):
    def setUp(self):
        self.redis = MockRedis()

    def test_obtain_lock(self):
        from ModestMaps.Core import Coordinate
        from tileserver.cache import RedisCache

        coord = Coordinate(0, 0, 0)

        c = RedisCache(self.redis)
        try:
            c.obtain_lock(coord)
        finally:
            c.release_lock(coord)

    def test_obtain_lock_already_locked(self):
        from ModestMaps.Core import Coordinate
        from tileserver.cache import RedisCache, LockTimeout

        coord = Coordinate(0, 0, 0)

        c = RedisCache(self.redis)
        try:
            c.obtain_lock(coord)
            with self.assertRaises(LockTimeout):
                c.obtain_lock(coord, timeout=1)
        finally:
            c.release_lock(coord)

        c.obtain_lock(coord)

    def test_set_get(self):
        from ModestMaps.Core import Coordinate
        from tileserver.cache import RedisCache

        coord = Coordinate(0, 0, 0)
        tile_data = 'hello world'

        c = RedisCache(self.redis)
        c.set(coord, tile_data)
        actual_data = c.get(coord)

        self.assertEquals(tile_data, actual_data)
        self.redis.delete(c.generate_key('data', coord))
