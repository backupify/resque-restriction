module Resque
  class Job

    class << self

      alias_method :origin_reserve, :reserve

      # Wrap reserve so we can move a job to restriction queue if it is restricted
      # This needs to be a class method
      def reserve(queue)
        queue_size = Resque.size(queue)

        # Try up to N times to get a unrestricted job from the queue
        count = [queue_size, Plugins::Restriction.restriction_queue_batch_size].min
        count.times do |i|

          resque_job = origin_reserve(queue)
          return nil unless resque_job

          job_class = resque_job.payload_class
          job_args = resque_job.args

          # drop through to original Job::Reserve if not restriction queue
          return resque_job unless job_class.is_a?(Plugins::Restriction)

          # Return nil to move on to next queue if job is restricted, otherwise
          # return the job to be performed
          if job_class.restricted?(*job_args)
            job_class.push_to_restriction_queue(queue, *job_args)
          else
            return resque_job
          end

        end

        return nil
      end

    end

    alias_method :origin_perform, :perform

    # Wrap perform so we can clear restriction locks after running.
    # This needs to be a instance method
    def perform
      begin
        return origin_perform
      ensure
        payload_class.release_restriction(args) if payload_class.is_a?(Plugins::Restriction)
      end
    end

  end
end
