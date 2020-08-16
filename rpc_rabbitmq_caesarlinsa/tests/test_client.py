from rpc_rabbitmq_caesarlinsa.rabbitmq_entity import Target
from rpc_rabbitmq_caesarlinsa.connection import call, cast

target = Target("task_exchange", "task", "task_queue")
cast(target, "test")
print("######################")
print(call(target, "test"))
