require 'rubygems'
require 'spec/autorun'
require 'mocha'

spec_dir = File.dirname(File.expand_path(__FILE__))
$LOAD_PATH << File.expand_path("#{spec_dir}/../lib")

#
# make sure we can run redis
#

if !system("which redis-server")
  puts '', "** can't find `redis-server` in your path"
  puts "** try running `sudo rake install`"
  abort ''
end

#
# start our own redis when the tests start,
# kill it when they end
#
REDIS_CMD = "redis-server #{spec_dir}/redis-test.conf"

puts "Starting redis for testing at localhost:9736..."
puts `cd #{spec_dir}; #{REDIS_CMD}`

require 'resque'
Resque.redis = 'localhost:9736'
require 'resque-restriction'

# Schedule the redis server for shutdown when tests are all finished.
at_exit do
  pid = File.read("#{spec_dir}/redis.pid").to_i rescue nil
  system ("kill -9 #{pid}") if pid != 0
  File.delete("#{spec_dir}/redis.pid") rescue nil
  File.delete("#{spec_dir}/redis-server.log") rescue nil
  File.delete("#{spec_dir}/dump.rdb") rescue nil
end


##
# Helper to perform job classes
#
module PerformJob

  def run_resque_job(job_class, *job_args)
    opts = job_args.last.is_a?(Hash) ? job_args.pop : {}
    queue = opts[:queue] || Resque.queue_from_class(job_class)

    Resque::Job.create(queue, job_class, *job_args)

    run_resque_queue(queue, opts)
  end

  def run_resque_queue(queue, opts={})
    worker = Resque::Worker.new(queue)
    worker.very_verbose = true if opts[:verbose]

    if opts[:fork]
      # do a single job then shutdown
      def worker.done_working
        super
        shutdown
      end
      worker.work(0.01)
    else
      job = worker.reserve
      worker.perform(job)
    end
  end

end

class OneDayRestrictionJob < Resque::Plugins::RestrictionJob
  restrict :per_day => 100

  @queue = 'normal'
  
  def self.perform(args)
  end
end

class OneHourRestrictionJob < Resque::Plugins::RestrictionJob
  restrict :per_hour => 10

  @queue = 'normal'

  def self.perform(args)
  end
end

class IdentifiedRestrictionJob < Resque::Plugins::RestrictionJob
  restrict :per_hour => 10

  @queue = 'normal'

  def self.restriction_identifier(*args)
    [self.to_s, args.first].join(":")
  end

  def self.perform(*args)
  end
end

class ConcurrentRestrictionJob < Resque::Plugins::RestrictionJob
  restrict :concurrent => 1

  @queue = 'normal'

  def self.perform(*args)
    sleep 0.2
  end
end

class MultipleRestrictionJob < Resque::Plugins::RestrictionJob
  restrict :per_hour => 10, :per_300 => 2

  @queue = 'normal'

  def self.perform(args)
  end
end

class MultiCallRestrictionJob < Resque::Plugins::RestrictionJob
  restrict :per_hour => 10
  restrict :per_300 => 2

  @queue = 'normal'

  def self.perform(args)
  end
end

class UnrestrictedJob
  @queue = 'normal'

  def self.perform(*args)
  end
end
