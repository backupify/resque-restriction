module Resque
  module Plugins

    # To configure resque restriction, add something like the following to an initializer (defaults shown)
    #
    #    Resque::Plugins::Restriction.configure do |config|
    #      # The prefix to append to the queue for its restriction queue
    #      config.restriction_queue_prefix = 'restriction'
    #      # how many items to scan in the restriction queue at a time
    #      config.restriction_queue_batch_size = 1000
    #      # The prefix for the key used to lock the queue
    #      config.restriction_queue_lock_prefix = "'restriction.lock'
    #      # The lock timeout for the restriction queue lock
    #      config.restriction_queue_lock_timeout = 10
    #    end

    module Restriction

      class << self
        # optional
        attr_accessor :restriction_queue_prefix, :restriction_queue_batch_size
        attr_accessor :restriction_queue_lock_prefix, :restriction_queue_lock_timeout
      end

      # default values
      self.restriction_queue_prefix = 'restriction'
      self.restriction_queue_batch_size = 1000
      self.restriction_queue_lock_prefix = 'restriction.lock'
      self.restriction_queue_lock_timeout = 60

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

      def source_queue=(queue_name)
        @source_queue = queue_name
      end

      def source_queue
        @source_queue
      end

      def restrict(options={})
        settings.merge!(options)
      end

      def before_perform_restriction(*args)
        keys_decremented = []
        settings.each do |period, number|
          key = redis_key(period, *args)

          # first try to set period key to be the total allowed for the period
          # if we get a 0 result back, the key wasn't set, so we know we are
          # already tracking the count for that period'
          period_active = ! Resque.redis.setnx(key, number.to_i - 1)

          # If we are already tracking that period, then decrement by one to
          # see if we are allowed to run, pushing to restriction queue to run
          # later if not.  Note that the value stored is the number of outstanding
          # jobs allowed, thus we need to reincrement if the decr discovers that
          # we have bypassed the limit
          if period_active
            value = Resque.redis.decrby(key, 1).to_i
            keys_decremented << key
            if value < 0
              # reincrement the keys if one of the periods triggers DontPerform so
              # that we accurately track capacity
              keys_decremented.each {|k| Resque.redis.incrby(k, 1) }
              Resque.push restriction_queue_name(source_queue), :class => to_s, :args => args
              raise Resque::Job::DontPerform
            end
          end
        end
      end

      def after_perform_restriction(*args)
        if settings[:concurrent]
          key = redis_key(:concurrent, *args)
          Resque.redis.incrby(key, 1)
        end
      end

      def on_failure_restriction(ex, *args)
        after_perform_restriction(*args)
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

      def seconds(period)
        if SECONDS.keys.include? period
          SECONDS[period]
        else
          period.to_s =~ /^per_(\d+)$/ and $1
        end
      end

      # if job is still restricted, push back to restriction queue, otherwise push
      # to real queue.  Since the restrictions will be checked again when run from
      # real queue, job will just get pushed back onto restriction queue then if
      # restriction conditions have changed
      def restricted?(queue, *args)
        has_restrictions = false
        settings.each do |period, number|
          key = redis_key(period, *args)
          value = Resque.redis.get(key)
          has_restrictions = value && value != "" && value.to_i <= 0
          break if has_restrictions
        end
        return has_restrictions
      end

    end

    class RestrictionJob
      extend Restriction
    end

  end
end
