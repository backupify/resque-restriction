module Resque
  module Plugins
    module Restriction

      module Job

        def self.extended(receiver)
          class << receiver
            alias reserve_without_restriction reserve
            alias reserve reserve_with_restriction
          end
        end

        # Wrap reserve so we can move a job to restriction queue if it is restricted
        # This needs to be a class method
        def reserve_with_restriction(queue)
          queue_size = Resque.size(queue)

          # Try up to N times to get a unrestricted job from the queue
          count = [queue_size, Plugins::Restriction.restriction_queue_batch_size].min
          count.times do |i|

            resque_job = reserve_without_restriction(queue)
            return nil unless resque_job

            job_class = resque_job.payload_class
            job_args = resque_job.args

            # return to work on job if not a restricted job
            return resque_job unless job_class.is_a?(Plugins::Restriction)

            # Move on to next if job is restricted, otherwise
            # return the job to be performed
            if job_class.restricted?(*job_args)
              job_class.push_to_restriction_queue(queue, *job_args)
            else
              return resque_job
            end

          end

          # Return nil to move on to next queue if we couldn't get a job after batch_size tries
          return nil
        end

      end

      module Worker

        def self.included(receiver)
          receiver.class_eval do
            alias done_working_without_restriction done_working
            alias done_working done_working_with_restriction
          end
        end

        # Wrap done_working so we can clear restriction locks after running.
        # We do this here instead of in Job.perform to improve odds of completing successfully
        # by running in the worker parent in case the child segfaults or something.
        # This needs to be a instance method
        def done_working_with_restriction
          begin
            payload = job['payload']
            job_class = Resque.constantize(payload['class'])
            job_args = payload['args']
            job_class.release_restriction(*job_args) if job_class.is_a?(Plugins::Restriction)
          ensure
            return done_working_without_restriction
          end
        end
        
      end

    end
  end
end
