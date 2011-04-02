require File.expand_path('../../spec_helper', __FILE__)

describe Resque::Job do
  include PerformJob

  before(:each) do
    Resque.redis.flushall
  end

  it "should repush restriction queue when reserve" do
    Resque.push('restriction_normal', :class => 'OneHourRestrictionJob', :args => ['any args'])
    Resque::Job.reserve('restriction_normal').should == Resque::Job.new('restriction_normal', {'class' => 'OneHourRestrictionJob', 'args' => ['any args']})
    Resque::Job.reserve('restriction_normal').should be_nil
    Resque::Job.reserve('normal').should be_nil
  end

  it "should push back to restriction queue when still restricted" do
    Resque.redis.set(OneHourRestrictionJob.redis_key(:per_hour), 10)
    Resque.push('restriction_normal', :class => 'OneHourRestrictionJob', :args => ['any args'])
    Resque::Job.reserve('restriction_normal').should be_nil
    Resque.pop('restriction_normal').should == {'class' => 'OneHourRestrictionJob', 'args' => ['any args']}
    Resque::Job.reserve('normal').should be_nil
  end

  it "should not repush when reserve normal queue" do
    Resque.push('normal', :class => 'OneHourRestrictionJob', :args => ['any args'])
    Resque::Job.reserve('normal').should == Resque::Job.new('normal', {'class' => 'OneHourRestrictionJob', 'args' => ['any args']})
    Resque::Job.reserve('normal').should be_nil
    Resque::Job.reserve('restriction_normal').should be_nil
  end

  it "should push batch_size times to restriction queue from normal" do
    Resque.redis.set(OneHourRestrictionJob.redis_key(:per_hour), 10)
    Resque::Plugins::Restriction.stub!(:restriction_queue_batch_size).and_return(3)
    [1, 2, 3, 4].each {|i| Resque.push('normal', :class => 'OneHourRestrictionJob', :args => [i]) }
    Resque.size('normal').should == 4
    Resque.size('restriction_normal').should == 0
    Resque::Job.reserve('normal')
    Resque.size('normal').should == 1
    Resque.size('restriction_normal').should == 3
    [4].each {|i| Resque.pop('normal').should == {'class' => 'OneHourRestrictionJob', 'args' => [i]} }
    [1, 2, 3].each {|i| Resque.pop('restriction_normal').should == {'class' => 'OneHourRestrictionJob', 'args' => [i]} }
  end

  it "should push back batch_size times to restriction queue" do
    Resque.redis.set(OneHourRestrictionJob.redis_key(:per_hour), 10)
    Resque::Plugins::Restriction.stub!(:restriction_queue_batch_size).and_return(3)
    [1, 2, 3, 4].each {|i| Resque.push('restriction_normal', :class => 'OneHourRestrictionJob', :args => [i]) }
    Resque.size('normal').should == 0
    Resque.size('restriction_normal').should == 4
    Resque::Job.reserve('restriction_normal')
    Resque.size('restriction_normal').should == 4
    [4, 1, 2, 3].each {|i| Resque.pop('restriction_normal').should == {'class' => 'OneHourRestrictionJob', 'args' => [i]} }
  end

  it "should only push back queue length times to restriction queue" do
    Resque.redis.set(OneHourRestrictionJob.redis_key(:per_hour), 10)
    Resque::Plugins::Restriction.stub!(:restriction_queue_batch_size).and_return(3)
    2.times { Resque.push('restriction_normal', :class => 'OneHourRestrictionJob', :args => ['any args']) }
    Resque.size('restriction_normal').should == 2
    OneHourRestrictionJob.should_receive(:push_to_restriction_queue).exactly(2).times
    Resque::Job.reserve('restriction_normal')
  end


  it "should move restricted job to queue based on source queue" do
    Resque.redis.set(OneHourRestrictionJob.redis_key(:per_hour), 99)
    run_resque_job(OneHourRestrictionJob, "any args", :queue => "foo_queue")
    Resque.size('foo_queue').should == 0
    Resque.size('normal').should == 0
    Resque.size('restriction_normal').should == 0
    Resque.size('restriction_foo_queue').should == 1
  end

  it "should not fail for plain job class" do
    Resque::Job.create(:normal_foo, UnrestrictedJob)
    worker = Resque::Worker.new("*")
    worker.work(0)
    Resque.redis.lrange("failed", 0, -1).size.should == 0
  end

  it "should not restrict plain job class" do
    Resque.push('normal', :class => 'UnrestrictedJob', :args => [1])
    UnrestrictedJob.expects(:restricted?).never
    Resque::Job.reserve('normal').should == Resque::Job.new('normal', {'class' => 'UnrestrictedJob', 'args' => [1]})
    Resque.size('restriction_normal').should == 0
    Resque.size('normal').should == 0
  end

  it "should not fail on empty queue" do
    Resque.size('restriction_normal').should == 0
    Resque.size('normal').should == 0
    Resque::Job.reserve('normal').should == nil
    Resque.size('restriction_normal').should == 0
    Resque.size('normal').should == 0
  end

end
