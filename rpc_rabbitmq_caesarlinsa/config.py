from rpc_rabbitmq_caesarlinsa.options import define, options

define(name="connection_pool_size", default=100, type=int)
define(name="rabbit_user", default="guest", type=str)
define(name="rabbit_password", default="guest", type=str)
define(name="rabbit_host", default="localhost", type=str)
define(name="rabbit_port", default="5672", type=str)
