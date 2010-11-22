module Resque
  class Job
    class <<self
      alias_method :origin_reserve, :reserve
      
      def reserve(queue)
        if queue =~ /^#{Plugins::Restriction::RESTRICTION_QUEUE_PREFIX}/
          # If processing the restriction queue, when poping and pushing to end,
          # we can't tell when we reach the original one, so just walk the length
          # of the queue so we don't run infinitely long
          Resque.size(queue).times do |i|
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
