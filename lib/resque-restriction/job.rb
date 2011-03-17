module Resque
  class Job
    class <<self
      alias_method :origin_reserve, :reserve
      
      def reserve(queue)
        if queue =~ /^#{Plugins::Restriction.restriction_queue_prefix}/
          
          lock_timeout = Plugins::Restriction.restriction_queue_lock_timeout
          lock_key = "#{Plugins::Restriction.restriction_queue_lock_prefix}.#{queue}"

          # acquire the lock to work on the restriction queue
          acquired_lock = Resque.redis.setnx(lock_key, Time.now.to_i + lock_timeout + 1)

          # If we don't acquire the lock, check the expiration as described
          # at http://redis.io/commands/setnx
          if ! acquired_lock
            # If expiration time is in the future, then don't process.  We do process if unset
            expiration_time = Resque.redis.get(lock_key)
            return nil if expiration_time.to_i > Time.now.to_i

            # if expiration time was in the future when we set it, then don't
            # process as someone beat us to it  We do process if unset
            expiration_time = Resque.redis.getset(lock_key, Time.now.to_i + lock_timeout + 1)
            return nil if expiration_time.to_i > Time.now.to_i
          end

          begin

            redis_queue = "queue:#{queue}"
            range = Resque.redis.lrange(redis_queue, 0, Plugins::Restriction.restriction_queue_batch_size - 1)
            range.each_with_index do |queue_entry, i|
              # For the job at the head of the queue, repush to restricition queue
              # if still restricted, otherwise we have a runnable job, so create it
              # and return
              payload = Resque.decode(queue_entry)
              if ! constantize(payload['class']).restricted?(queue, *payload['args'])
                removed = Resque.redis.lrem(redis_queue, 1, queue_entry).to_i
                if removed == 1
                  return new(queue, payload)
                else
                  return nil
                end
              end

            end

          ensure
            # don't delete expired keys as this is a race condition if expired
            # because it is handled above
            Resque.redis.del(lock_key) if Resque.redis.get(lock_key).to_i > Time.now.to_i
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
