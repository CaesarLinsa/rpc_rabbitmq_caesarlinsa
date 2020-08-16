import time
import socket
import itertools
from kombu import connection
import uuid
import threading
from rpc_rabbitmq_caesarlinsa.rabbitmq_entity import (Target,
                                                      TopicConsumer,
                                                      DirectConsumer,
                                                      TopicPublisher,
                                                      DirectPublisher)
from rpc_rabbitmq_caesarlinsa.connection_pool import get_connection_pool
from oslo_config import cfg

MSG_ID = 'msg_id'

conf = cfg.CONF

rabbit_url = cfg.StrOpt('RABBITMQ_CONNECTION_URL',
                        default='amqp://guest:guest@localhost:5672//',
                        help='rabbitmq login url.'),

conf.register_opts(rabbit_url)


class Connection(object):
    pool = None

    def __init__(self, connection_url=None):
        self.consumers = []
        self.connection_url = connection_url if connection_url \
            else conf.get("RABBITMQ_CONNECTION_URL")
        if not self.connection_url:
            raise Exception("connection url can't be None")
        self.reconnect()

    def _connect(self):
        try:
            self.connection = connection.Connection(self.connection_url)
            self.connection_errors = self.connection.connection_errors
            self.channel = self.connection.channel()
            self.connection.connect()
            self.consumer_num = itertools.count(1)
            for consumer in self.consumers:
                consumer.reconnect(self.channel)
        except Exception as e:
            print("connect broker caught an exception:%s" % str(e))
            raise e

    def reconnect(self, timeout=None):

        attempt = 0
        while True:
            try:
                self._connect()
                return
            except (IOError, self.connection_errors) as e:
                pass
            except Exception as e:
                if 'timeout' not in str(e):
                    raise
            attempt += 1
            if timeout is None:
                time.sleep(2)
            else:
                time.sleep(timeout)

    def ensure(self, error_callback, method, *args, **kwargs):
        while True:
            try:
                return method(*args, **kwargs)
            except (self.connection_errors, socket.timeout, IOError) as e:
                if error_callback:
                    error_callback(e)
            except Exception as e:
                if 'timeout' not in str(e):
                    raise
                if error_callback:
                    error_callback(e)
            self.reconnect()

    def declare_consumer(self, consumer_cls, target, callback):

        def _connect_error(exc):
            print("caught an error %s" % str(exc))

        def _declare_consumer():
            consumer = consumer_cls(self.channel, target, callback)
            self.consumer_num.next()
            self.consumers.append(consumer)
            return consumer

        return self.ensure(_connect_error, _declare_consumer)

    def iterconsume(self, limit=None, timeout=None):
        """Return an iterator that will consume from all queues/consumers."""

        def _error_callback(exc):
            if isinstance(exc, socket.timeout):
                print('Timed out waiting for RPC response: %s' % str(exc))
                raise exc
            else:
                print('Failed to consume message from queue: %s' % str(exc))
                self.do_consume = True

        def _consume():
            queues_head = self.consumers[:-1]
            queues_tail = self.consumers[-1]
            for queue in queues_head:
                queue.consume(nowait=True)
            queues_tail.consume(nowait=False)
            self.do_consume = False
            return self.connection.drain_events(timeout=timeout)

        for iteration in itertools.count(0):
            if limit and iteration >= limit:
                raise StopIteration
            yield self.ensure(_error_callback, _consume)

    def publisher_send(self, cls, target, msg, timeout=None):
        """Send to a publisher based on the publisher class."""

        def _error_callback(exc):
            print("Failed to publish message to topic "
                  "'%s" % str(exc))

        def _publish():
            publisher = cls(self.channel, target)
            publisher.publish(msg, timeout)

        self.ensure(_error_callback, _publish)

    def declare_direct_consumer(self, target, callback):
        self.declare_consumer(DirectConsumer, target, callback)

    def declare_topic_consumer(self, target, callback):
        self.declare_consumer(TopicConsumer, target, callback)

    def direct_send(self, target, msg, timeout=None):
        self.publisher_send(DirectPublisher, target, msg, timeout)

    def topic_send(self, target, msg, timeout=None):
        self.publisher_send(TopicPublisher, target, msg, timeout)

    def consume(self, limit=None, timeout=None):
        it = self.iterconsume(limit=limit, timeout=timeout)
        while True:
            try:
                it.next()
            except StopIteration:
                return

    def close(self):
        self.connection.release()
        self.connection = None

    def reset(self):
        self.channel.close()
        self.channel = self.connection.channel()
        self.consumers = []


class ConnectionContext(object):

    def __init__(self, connection_pool, pooled=True):

        self.connection = None
        self.connection_pool = connection_pool

        if pooled:
            self.connection = self.connection_pool.get()
        else:
            self.connection = self.connection_pool.connection_cls()

        self.pooled = pooled

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._done()

    def _done(self):

        if self.connection:
            if self.pooled:
                self.connection.reset()
                self.connection_pool.put(self.connection)
            else:
                try:
                    self.connection.close()
                except Exception:
                    pass

    def __getattr__(self, key):

        if self.connection:
            return getattr(self.connection, key)
        else:
            raise Exception("connection is None")


def _add_msg_id(msg):
    msg_id = uuid.uuid4().hex
    msg.update({MSG_ID: msg_id})


def _make_message(msg, method, args):
    msg = msg.update(method=method)
    if args:
        msg['args'] = dict()
        for argname, arg in args.iteritems():
            msg['args'][argname] = arg


def _call(target, method, args=None, timeout=None):
    msg = dict()
    _add_msg_id(msg)
    _make_message(msg, method, args)
    with ConnectionContext(get_connection_pool(Connection)) as conn:
        conn.topic_send(target, msg, timeout)


def cast(target, method, args=None, timeout=None):
    _call(target, method, args, timeout)


def call(target, method, args=None, timeout=None):
    msg = dict()
    _add_msg_id(msg)
    _make_message(msg, method, args)
    msg["reply"] = True
    waiter = ReplyWaiter(get_connection_pool(Connection), msg)
    with ConnectionContext(get_connection_pool(Connection)) as conn:
        conn.topic_send(target, msg, timeout)
        result = waiter.wait(timeout)
        return result


class ReplyWaiter(ConnectionContext):

    def __init__(self, connection_pool, msg):
        super(ReplyWaiter, self).__init__(connection_pool, pooled=True)
        self.target = self.get_target(msg)
        self.incoming = []
        self.conn_lock = threading.Lock()
        self.declare_direct_consumer(self.target, self)

    def get_target(self, msg):
        msg_id = msg.get(MSG_ID)
        return Target("exchange_%s" % msg_id,
                      "routing_key_%s" % msg_id,
                      "reply_%s" % msg_id)

    def __call__(self, message):
        self.incoming.append(message)

    def get_poll_data(self, timeout):
        while True:
            if self.incoming:
                return self.incoming.pop(0)
            self.connection.consume(limit=1,
                                    timeout=timeout)

    def wait(self, timeout):
        while True:
            with self.conn_lock:
                return self.get_poll_data(timeout)
