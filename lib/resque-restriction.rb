require 'resque'
require 'resque-restriction/job'
require 'resque-restriction/restriction_job'

server_ver = Resque.redis.info["redis_version"].split('.').collect{|x| x.to_i}
unsupported_version = (server_ver <=> [2, 2, 0]) < 0
raise "resque-restriction requires a redis-server version >= 2.2.0" if unsupported_version
