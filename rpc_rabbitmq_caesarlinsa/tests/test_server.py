from rpc_rabbitmq_caesarlinsa.server import Server
from rpc_rabbitmq_caesarlinsa.connection_pool import get_connection_pool
from rpc_rabbitmq_caesarlinsa.connection import Connection, ConnectionContext
from rpc_rabbitmq_caesarlinsa.rabbitmq_entity import Target
import time

target = Target("task_exchange", "task", "task_queue")


class TestEndPoint():

    def test(self):
        print("from server")
        return "from server"


endpoints = [TestEndPoint(), ]

with ConnectionContext(get_connection_pool(Connection)) as conn:
    s = Server(conn, target, endpoints)
    s.start()

    while True:
        time.sleep(1)
    s.stop()
