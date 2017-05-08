import errno
import os
import time
from contextlib import contextmanager
from string import zfill


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def clean_empty_parent_dirs(path, parent_dir=None):
    """
    Starting from a file or directory ``path``, recursively delete empty
    parent directories until a non-empty directory or ``parent_dir``
    is reached.

    This is like ``os.removedirs()`` but with a specified stop point.
    """
    if not os.path.exists(path):
        return

    if not os.path.isdir(path):
        path = os.path.dirname(path)

    while True:
        if parent_dir and path.endswith(parent_dir):
            return

        try:
            os.rmdir(path)
            path = os.path.dirname(path)
        except OSError:
            return


class LockTimeout(BaseException):
    pass


class RedisCache(object):
    def __init__(self, redis_client, **kwargs):
        self.client = redis_client
        self.timeout = kwargs.get('timeout') or 10
        self.key_prefix = kwargs.get('key_prefix') or 'tiles'

    def generate_key(self, key_type, coord):
        return '{}.{}.{}-{}-{}'.format(
            self.key_prefix,
            key_type,
            coord.zoom,
            coord.column,
            coord.row,
        )

    def obtain_lock(self, coord, **kwargs):
        """
        Obtains a lock based on the given tile coordinate. By default,
        it will wait/block ``timeout`` seconds before giving up and throwing
        a ``LockTimeout`` exception.

        :param coord   The tile Coordinate to lock on.
        :param expires Any existing lock older than ``expires`` seconds will
                       be considered invalid.
        :param timeout If another client has already obtained the lock for this
                       tile, sleep for a maximum of ``timeout`` seconds before
                       giving up and throwing a ``LockTimeout`` exception. A
                       value of 0 means to never wait.

        (https://chris-lamb.co.uk/posts/distributing-locking-python-and-redis)

        """
        key = self.generate_key('lock', coord)
        expires = kwargs.get('expires', 60)
        timeout = kwargs.get('timeout', 10)

        while timeout >= 0:
            expire_tstamp = time.time() + expires + 1

            if self.client.setnx(key, expires):
                # We gained the lock
                return

            current_value = self.client.get(key)

            # We found an expired lock and nobody raced us to replacing it
            if current_value and float(current_value) < time.time() and \
               self.client.getset(key, expire_tstamp) == current_value:
                    return

            timeout -= 1
            time.sleep(1)

        raise LockTimeout("Timeout whilst waiting for a lock")

    def release_lock(self, coord):
        key = self.generate_key('lock', coord)
        self.client.delete(key)

    @contextmanager
    def lock(self, coord, **kwargs):
        self.obtain_lock(coord, **kwargs)
        try:
            yield self
        finally:
            self.release_lock(coord)

    def set(self, coord, data):
        key = self.generate_key('data', coord)
        self.client.set(key, data)

    def get(self, coord):
        key = self.generate_key('data', coord)
        return self.client.get(key)


class FileCache(object):
    def __init__(self, file_prefix, **kwargs):
        self.prefix = file_prefix

    def generate_key(self, key_type, coord):
        x_fill = zfill(coord.column, 9)
        y_fill = zfill(coord.row, 9)

        return os.path.join(
            self.prefix,
            zfill(coord.zoom, 2),
            x_fill[0:3],
            x_fill[3:6],
            x_fill[6:9],
            y_fill[0:3],
            y_fill[3:6],
            '{}.{}'.format(y_fill[6:9], key_type),
        )

    def _acquire(self, key):
        try:
            with open(key, 'r'):
                return False
        except IOError:
            directory = os.path.dirname(key)
            mkdir_p(directory)
            with open(key, 'w'):
                return True

    def obtain_lock(self, coord, **kwargs):
        """
        Obtains a lock based on the given tile coordinate. By default,
        it will wait/block ``timeout`` seconds before giving up and throwing
        a ``LockTimeout`` exception.

        :param coord   The tile Coordinate to lock on.
        :param expires Any existing lock older than ``expires`` seconds will
                       be considered invalid.
        :param timeout If another client has already obtained the lock for this
                       tile, sleep for a maximum of ``timeout`` seconds before
                       giving up and throwing a ``LockTimeout`` exception. A
                       value of 0 means to never wait.
        """
        key = self.generate_key('lock', coord)
        expires = kwargs.get('expires', 60)
        timeout = kwargs.get('timeout', 10)

        while timeout >= 0:
            expires = time.time() + expires + 1

            if self._acquire(key):
                # We gained the lock
                return

            timeout -= 1
            time.sleep(1)

        raise LockTimeout("Timeout whilst waiting for a lock")

    def release_lock(self, coord):
        key = self.generate_key('lock', coord)
        try:
            os.remove(key)
        except OSError as e:
            # errno.ENOENT = no such file or directory
            if e.errno != errno.ENOENT:
                # re-raise exception if a different error occurred
                raise

    @contextmanager
    def lock(self, coord, **kwargs):
        try:
            self.obtain_lock(coord, **kwargs)
            yield
        finally:
            self.release_lock(coord)

    def set(self, coord, data):
        key = self.generate_key('data', coord)
        directory = os.path.dirname(key)
        mkdir_p(directory)

        with open(key, 'w') as f:
            f.write(data)

    def get(self, coord):
        key = self.generate_key('data', coord)
        with open(key, 'r') as f:
            return f.read()
