module Resque
  module Plugins

    # To configure resque restriction, add something liker the following to an initializer (defaults show)
    #
    #    Resque::Plugins::Restriction.configure do |config|
    #      # The prefix to append to the queue for its restriction queue
    #      config.restriction_queue_prefix = 'restriction'
    #      # how many items to scan in the restriction queue at a time - should be large when you have few workers
    #      config.restriction_queue_batch_size = 1
    #      # how long before expiring concurrent keys - should be larger than your longest running job
    #      config.concurrent_key_expire = 60*60*3
    #    end

    module Restriction

      class << self
        # optional
        attr_accessor :restriction_queue_prefix, :restriction_queue_batch_size
        attr_accessor :concurrent_key_expire
      end

      # default values
      self.restriction_queue_prefix = 'restriction'
      self.restriction_queue_batch_size = 1
      self.concurrent_key_expire = 60*60*3

      def self.configure
        yield self
      end

      SECONDS = {
        :per_minute => 60,
        :per_hour => 60*60,
        :per_day => 24*60*60,
        :per_week => 7*24*60*60,
        :per_month => 31*24*60*60,
        :per_year => 366*24*60*60
      }

      def settings
        @options ||= {}
      end

      def restrict(options={})
        settings.merge!(options)
      end

      def redis_key(period, *args)
        period_str = case period
                     when :concurrent then "*"
                     when :per_minute, :per_hour, :per_day, :per_week then (Time.now.to_i / SECONDS[period]).to_s
                     when :per_month then Date.today.strftime("%Y-%m")
                     when :per_year then Date.today.year.to_s
                     else period.to_s =~ /^per_(\d+)$/ and (Time.now.to_i / $1.to_i).to_s end
        [self.restriction_identifier(*args), period_str].compact.join(":")
      end

      def restriction_identifier(*args)
        self.to_s
      end

      def restriction_queue_name(queue)
        queue_name = queue || Resque.queue_from_class(self)
        queue_name = "#{Plugins::Restriction.restriction_queue_prefix}_#{queue_name}" if queue_name !~ /^#{Plugins::Restriction.restriction_queue_prefix}/
        return queue_name
      end

      def key_expiration(period)
        expiration = nil

        if SECONDS.keys.include? period
          expiration = SECONDS[period]
        elsif period.to_s =~ /^per_(\d+)$/
           expiration = $1.to_i
        else
          expiration = Plugins::Restriction.concurrent_key_expire
        end

        return expiration
      end

      # Tells us if the job is restricted for the given job args
      #
      def restricted?(*args)
        has_restrictions = false
        keys_incremented = []

        settings.each do |period, number|
          key = redis_key(period, *args)

          # increment by one to see if we are allowed to run
          value = Resque.redis.incr(key)
          keys_incremented << key

          expire = key_expiration(period)
          Resque.redis.expire(key, expire)

          has_restrictions |= (value > number)
        end

        # reset the keys if we were restricted so we release the locks
        # otherwise we are runnable, so keep the locks till after we run
        keys_incremented.each {|k| Resque.redis.decr(k) } if has_restrictions

        return has_restrictions
      end

      def push_to_restriction_queue(source_queue, *args)
        Resque.push(restriction_queue_name(source_queue), :class => to_s, :args => args)
      end

      def release_restriction(*args)
        # All periods but concurrent use a key based on their period
        # for tracking counts, so when the period is up they move on
        # to a new key.  Concurrent period is the exception since there
        # is no time period associated with it, so we have to explicitly
        # decrement the count after a job has run
        if settings[:concurrent]
          key = redis_key(:concurrent, *args)
          Resque.redis.decr(key)
        end
      end

    end

    class RestrictionJob
      extend Restriction
    end

  end
end
