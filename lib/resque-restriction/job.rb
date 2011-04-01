module Resque
  class Job

    class << self

      alias_method :origin_reserve, :reserve

      # Wrap reserve so we can move a job to restriction queue if it is restricted
      # This needs to be a class method
      def reserve(queue)
        queue_size = Resque.size(queue)
        resque_job = origin_reserve(queue)
        return nil unless resque_job

        job_class = resque_job.payload_class
        job_args = resque_job.args

        # drop through to original Job::Reserve if not restriction queue
        return resque_job unless job_class.is_a?(Plugins::Restriction)

        # Try N times to get a unrestricted job from the queue
        count = [queue_size, Plugins::Restriction.restriction_queue_batch_size].min
        count.times do |i|

          # Return nil to move on to next queue if job is restricted, otherwise
          # return the job to be performed
          if job_class.restricted?(*job_args)
            job_class.push_to_restriction_queue(*job_args)
          else
            return resque_job
          end

        end

        return nil
      end

    end

    alias_method :origin_perform, :perform

    # Wrap perform so we can track the source queue as well as clear
    # restriction locks after running.
    # This needs to be a instance method
    def perform
      # This lets job classes that use resque-restriction know what queue the job
      # was taken off of, so that it can be pushed to a restriction variant of that
      # queue when restricted.  Fixes the problem where a job needs to be queued to a queue
      # that is not the same as the one declared in the class
      payload_class.source_queue = queue if payload_class.respond_to?(:source_queue=)
      begin
        return origin_perform
      ensure
        payload_class.release_restriction(args) if payload_class.is_a?(Plugins::Restriction)
      end
    end

  end
end
