# _-_coding:utf-8 _-_
from rpc_rabbitmq_caesarlinsa.listener import AMQPListener
from rpc_rabbitmq_caesarlinsa.rabbitmq_entity import Target


class Server(object):
    def __init__(self, connection, target, endpoints):
        self.conn = connection
        self.listener = AMQPListener(self.conn, target)
        self.target = target
        self.endpoints = endpoints
        self.running = False

    def start(self):
        self.conn.declare_topic_consumer(self.target, self.listener)
        self.running = True
        while self.running:
            self._dispatch(self.listener.poll())

    def stop(self):
        self.running = False

    def _dispatch(self, msg_handler):
        for endpoint in self.endpoints:
            message = msg_handler.msg
            method = message.get("method")
            args = message.get("args", {})
            if hasattr(endpoint, method):
                result = self.__dispatch(endpoint, method, args)
                if message.get("reply"):
                    msg_id = msg_handler.msg_id
                    target = Target("exchange_%s" % msg_id,
                                    "routing_key_%s" % msg_id,
                                    "reply_%s" % msg_id)
                    # todo 此处不加休眠，虽然已经发送，
                    # 但是客户端队列中没有，怀疑可能队列没有
                    # 创建
                    import time
                    time.sleep(0.1)
                    self.conn.direct_send(target, result)

    def __dispatch(self, endpoint, method, args):
        new_args = dict()
        for argname, arg in args.iteritems():
            new_args[argname] = arg
        result = getattr(endpoint, method)(**new_args)
        return result
