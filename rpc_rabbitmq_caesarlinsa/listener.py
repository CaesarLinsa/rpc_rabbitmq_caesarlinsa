# _-_coding: utf-8 _-_

MSG_ID = 'msg_id'


class MessageHandler(object):

    def __init__(self, listener, msg_id, msg):
        self.listener = listener
        self.msg_id = msg_id
        self.msg = msg


class AMQPListener(object):

    def __init__(self, connection, target):
        self.target = target
        self.connection = connection
        self.incoming = []

    def __call__(self, message):
        msg_id = message.pop(MSG_ID)
        self.incoming.append(MessageHandler(self, msg_id, message))

    def poll(self):
        while True:
            if self.incoming:
                return self.incoming.pop()
            self.connection.consume(limit=1)
