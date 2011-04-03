require File.join(File.dirname(__FILE__) + '/../spec_helper')

describe Resque::Plugins::RestrictionJob do
  it "should follow the convention" do
    Resque::Plugin.lint(Resque::Plugins::RestrictionJob)
  end

  context "redis_key" do
    it "should get redis_key with different period" do
      Resque::Plugins::RestrictionJob.redis_key(:per_minute).should == "Resque::Plugins::RestrictionJob:#{Time.now.to_i / 60}"
      Resque::Plugins::RestrictionJob.redis_key(:per_hour).should == "Resque::Plugins::RestrictionJob:#{Time.now.to_i / (60*60)}"
      Resque::Plugins::RestrictionJob.redis_key(:per_day).should == "Resque::Plugins::RestrictionJob:#{Time.now.to_i / (24*60*60)}"
      Resque::Plugins::RestrictionJob.redis_key(:per_month).should == "Resque::Plugins::RestrictionJob:#{Date.today.strftime("%Y-%m")}"
      Resque::Plugins::RestrictionJob.redis_key(:per_year).should == "Resque::Plugins::RestrictionJob:#{Date.today.year}"
    end

    it "should accept customization" do
      Resque::Plugins::RestrictionJob.redis_key(:per_1800).should == "Resque::Plugins::RestrictionJob:#{Time.now.to_i / 1800}"
      Resque::Plugins::RestrictionJob.redis_key(:per_7200).should == "Resque::Plugins::RestrictionJob:#{Time.now.to_i / 7200}"
    end
  end
  
  context "settings" do
    it "get correct number to restriction jobs" do
      OneDayRestrictionJob.settings.should == {:per_day => 100}
      OneHourRestrictionJob.settings.should == {:per_hour => 10}
      MultipleRestrictionJob.settings.should == {:per_hour => 10, :per_300 => 2}
      MultiCallRestrictionJob.settings.should == {:per_hour => 10, :per_300 => 2}
    end

    it "should allow getting global config" do
      Resque::Plugins::Restriction.restriction_queue_batch_size.should == 1
      Resque::Plugins::Restriction.configure do |config|
        config.restriction_queue_batch_size = 15
      end
      Resque::Plugins::Restriction.restriction_queue_batch_size.should == 15
    end
  end
  
  context "#restriction_queue_name" do
    it "adds restriction to class without source_queue" do
      OneHourRestrictionJob.restriction_queue_name(nil).should == "restriction_normal"
    end

    it "adds restriction to class with source_queue" do
      OneHourRestrictionJob.restriction_queue_name("foobar").should == "restriction_foobar"
    end

    it "does not add restriction to class with already restricted source_queue" do
      OneHourRestrictionJob.restriction_queue_name("restriction_foobar").should == "restriction_foobar"
    end

  end

  context "resque" do
    include PerformJob

    before(:each) do
      Resque.redis.flushall
    end
    
    it "should set execution number and increment it when one job first executed" do
      run_resque_job(OneHourRestrictionJob, "any args")
      Resque.redis.get(OneHourRestrictionJob.redis_key(:per_hour)).should == "1"
      Resque.size(:normal).should == 0
      Resque.size(:restriction_normal).should == 0
    end

    it "should set expire on key job executed" do
      run_resque_job(ConcurrentRestrictionJob)
      ttl = Resque.redis.ttl(ConcurrentRestrictionJob.redis_key(:concurrent))
      actual = Resque::Plugins::Restriction.concurrent_key_expire
      ttl.should > actual - 2
      ttl.should <= actual

      run_resque_job(OneHourRestrictionJob, "any args")
      ttl = Resque.redis.ttl(OneHourRestrictionJob.redis_key(:per_hour))
      actual = 60*60
      ttl.should > actual - 2
      ttl.should <= actual

      run_resque_job(OneDayRestrictionJob, "any args")
      ttl = Resque.redis.ttl(OneDayRestrictionJob.redis_key(:per_day))
      actual = 60*60*24
      ttl.should > actual - 2
      ttl.should <= actual
    end


    it "should use restriction_identifier to set exclusive execution counts" do
      run_resque_job(IdentifiedRestrictionJob, 1)
      run_resque_job(IdentifiedRestrictionJob, 1)
      run_resque_job(IdentifiedRestrictionJob, 2)
      Resque.redis.get(IdentifiedRestrictionJob.redis_key(:per_hour, 1)).should == "2"
      Resque.redis.get(IdentifiedRestrictionJob.redis_key(:per_hour, 2)).should == "1"
    end

    it "should increment execution number when one job executed" do
      Resque.redis.set(OneHourRestrictionJob.redis_key(:per_hour), 6)
      run_resque_job(OneHourRestrictionJob, "any args")
      Resque.redis.get(OneHourRestrictionJob.redis_key(:per_hour)).should == "7"
    end

    it "should decrement execution number when concurrent job completes" do
      t = Thread.new do
        run_resque_job(ConcurrentRestrictionJob)
      end
      sleep 0.1
      Resque.redis.get(ConcurrentRestrictionJob.redis_key(:concurrent)).should == "1"
      t.join
      Resque.redis.get(ConcurrentRestrictionJob.redis_key(:concurrent)).should == "0"
    end

    it "should decrement execution number when concurrent job fails" do
      run_resque_job(ConcurrentRestrictionJob, "bad", :verbose => true)
      Resque.redis.get(ConcurrentRestrictionJob.redis_key(:concurrent)).should == "0"
    end

    it "should put the job into restriction queue when execution count above limit" do
      Resque.redis.set(OneHourRestrictionJob.redis_key(:per_hour), 99)
      run_resque_job(OneHourRestrictionJob, "any args", :queue => 'normal')
      Resque.redis.get(OneHourRestrictionJob.redis_key(:per_hour)).should == "99"
      Resque.size(:normal).should == 0
      Resque.size(:restriction_normal).should == 1
      Resque.pop("restriction_normal").should == {"class" => "OneHourRestrictionJob", "args" => ["any args"]}
    end

    it "should stop running after first restriction is met" do
      run_resque_job(MultipleRestrictionJob, "any args")
      Resque.redis.get(MultipleRestrictionJob.redis_key(:per_hour)).should == "1"
      Resque.redis.get(MultipleRestrictionJob.redis_key(:per_300)).should == "1"
      run_resque_job(MultipleRestrictionJob, "any args")
      Resque.size(:restriction_normal).should == 0
      run_resque_job(MultipleRestrictionJob, "any args")
      Resque.size(:restriction_normal).should == 1
      Resque.redis.get(MultipleRestrictionJob.redis_key(:per_hour)).should == "2"
      Resque.redis.get(MultipleRestrictionJob.redis_key(:per_300)).should == "2"
    end

  end
end
