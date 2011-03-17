module Resque
  class Job
    class <<self
      alias_method :origin_reserve, :reserve
      
      def reserve(queue)
        if queue =~ /^#{Plugins::Restriction.restriction_queue_prefix}/
          # If processing the restriction queue, when popping and pushing to end,
          # we can't tell when we reach the original one, so just walk up to N items
          # of the queue so we don't run infinitely long.
          # After N items, a worker will give up and try another queue.
          # This way, in aggregate, all items in a restriction queue will get processed, but
          # multiple workers won't get tied up while processing a large queue
          begin
            # prevent too many workers from processing restriction queue - with large queues
            # and many workers, end up with large memory fragmentation in redis due to all
            # the popping/pushing
            Resque.redis.setnx(Plugins::Restriction.scan_limit_key, Plugins::Restriction.scan_limit)
            limit = Resque.redis.decr(Plugins::Restriction.scan_limit_key)
            return nil if limit < 0

            count = [Resque.size(queue), Plugins::Restriction.restriction_queue_batch_size].min
            count.times do |i|
              # For the job at the head of the queue, repush to restricition queue
              # if still restricted, otherwise we have a runnable job, so create it
              # and return
              payload = Resque.pop(queue)
              if payload
                if ! constantize(payload['class']).repush(queue, *payload['args'])
                  return new(queue, payload)
                end
              end
            end

          ensure
            Resque.redis.incr(Plugins::Restriction.scan_limit_key)
          end

          return nil
        else
          # drop through to original Job::Reserve if not restriction queue
          origin_reserve(queue)
        end
      end

    end

    alias_method :origin_perform, :perform

    def perform
      # This lets job classes that use resque-restriction know what queue the job
      # was taken off of, so that it can be pushed to a restriction variant of that
      # queue when restricted.  Fixes the problem where a job needs to be queued to a queue
      # that is not the same as the one declared in the class
      payload_class.source_queue = queue if payload_class.respond_to?(:source_queue=)
      origin_perform
    end

  end
end
