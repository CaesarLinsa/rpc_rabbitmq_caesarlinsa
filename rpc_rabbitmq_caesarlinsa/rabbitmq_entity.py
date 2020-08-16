from kombu import Queue, Exchange
from kombu.messaging import Producer

__all__ = ['Target',
           'DirectPublisher',
           'TopicPublisher',
           'DirectConsumer',
           'TopicConsumer']


class Target(object):

    def __init__(self, exchange_name, routing_key, queue_name):
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.queue_name = queue_name


class BasePublisher(object):
    EXCHANGE_TYPE = None

    def __init__(self, channel, target, **kwargs):
        self.channel = channel
        self.target = target
        self.options = {'durable': False,
                        'queue_arguments': {'x-ha-policy': 'all'},
                        'auto_delete': True,
                        'exclusive': False}
        self.options.update(kwargs)

    def publish(self, data, timeout=None):
        p = Producer(channel=self.channel,
                     exchange=Exchange(self.target.exchange_name, type=self.EXCHANGE_TYPE,
                                       durable=self.options['durable'],
                                       auto_delete=self.options['auto_delete']),
                     routing_key=self.target.routing_key)
        if timeout:
            p.publish(data, headers={'ttl': (timeout * 1000)})
        else:
            p.publish(data)


class DirectPublisher(BasePublisher):
    EXCHANGE_TYPE = 'direct'


class TopicPublisher(BasePublisher):
    EXCHANGE_TYPE = 'topic'


class BaseConsumer(object):
    EXCHANGE_TYPE = None

    def __init__(self, channel, target, callback, **kwargs):
        self.target = target
        self.channel = channel
        self.callback = callback
        self.options = {'durable': False,
                        'queue_arguments': {'x-ha-policy': 'all'},
                        'auto_delete': True,
                        'exclusive': False}
        self.options.update(kwargs)

    def consume(self, nowait=False):
        q = Queue(self.target.queue_name,
                  exchange=Exchange(self.target.exchange_name, type=self.EXCHANGE_TYPE,
                                    durable=self.options['durable'],
                                    auto_delete=self.options['auto_delete']),
                  routing_key=self.target.routing_key,
                  channel=self.channel,
                  **self.options)
        q.declare(nowait)

        def _callback(raw_message):
            message = self.channel.message_to_python(raw_message)
            self._callback_handler(message, self.callback)

        q.consume(callback=_callback)

    def get_channel(self):
        return self.channel

    def _callback_handler(self, message, callback):

        ack_msg = False
        try:
            msg = message.payload
            callback(msg)
            ack_msg = True
        except Exception as e:
            print("caught an exception when deal with "
                  "callback msg %s" % str(e))
        finally:
            if ack_msg:
                message.ack()
            else:
                message.reject()


class DirectConsumer(BaseConsumer):
    EXCHANGE_TYPE = 'direct'


class TopicConsumer(BaseConsumer):
    EXCHANGE_TYPE = 'topic'
