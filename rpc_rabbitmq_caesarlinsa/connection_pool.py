# _-_coding:utf-8 _-_
import abc
import collections
import threading


class Pool(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, max_size=10):
        super(Pool, self).__init__()

        self.max_size = max_size
        self._current_size = 0
        self._cond = threading.Condition()

        self._items = collections.deque()

    def put(self, item):
        """
        使用完成后，对象放入池中
        """
        with self._cond:
            self._items.appendleft(item)
            self._cond.notify()

    def get(self):

        with self._cond:
            while True:
                try:
                    # 如果池中存在，则返回
                    return self._items.popleft()
                except IndexError:
                    pass

                if self._current_size < self.max_size:
                    self._current_size += 1
                    break
                # 创建已经到最大数量对象，但是池中没有连接对象时，等待1秒
                self._cond.wait(timeout=1)
        # 不存在则创建
        with self._cond:
            try:
                return self.create()
            except Exception:
                with self._cond:
                    self._current_size -= 1
                raise

    def iter_free(self):

        with self._cond:
            while True:
                try:
                    yield self._items.popleft()
                except IndexError:
                    break

    @abc.abstractmethod
    def create(self):
        """创建连接对象"""


class ConnectionPool(Pool):
    def __init__(self, connection_cls, connection_pool_size=100):
        self.connection_cls = connection_cls
        super(ConnectionPool, self).__init__(connection_pool_size)

    def create(self):
        return self.connection_cls()

    def close(self):
        for item in self.iter_free():
            item.close()


_pool_create_lock = threading.Lock()


def get_connection_pool(connection_cls):
    with _pool_create_lock:
        if not connection_cls.pool:
            connection_cls.pool = ConnectionPool(connection_cls)
        return connection_cls.pool
