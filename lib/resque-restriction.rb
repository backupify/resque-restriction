require 'resque'
require 'resque-restriction/job'
require 'resque-restriction/restriction_job'

unsupported_version = false
begin
  server_ver = Resque.redis.info["redis_version"].split('.').collect{|x| x.to_i}
  unsupported_version = (server_ver <=> [2, 2, 0]) < 0
rescue
end

raise "resque-restriction requires a redis-server version >= 2.2.0" if unsupported_version
