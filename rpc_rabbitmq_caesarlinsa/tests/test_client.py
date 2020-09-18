from rpc_rabbitmq_caesarlinsa.rabbitmq_entity import Target
from rpc_rabbitmq_caesarlinsa.connection import call, cast
from rpc_rabbitmq_caesarlinsa.options import parse_config_file

parse_config_file("./config")
target = Target("task_exchange", "task", "task_queue")
cast(target, "test")
print("######################")
print(call(target, "test"))
