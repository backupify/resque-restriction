require File.join(File.dirname(__FILE__) + '/../spec_helper')

describe Resque::Job do
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
    Resque.redis.set(OneHourRestrictionJob.redis_key(:per_hour), -1)
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

  it "should only push back queue_length times to restriction queue" do
    Resque.redis.set(OneHourRestrictionJob.redis_key(:per_hour), -1)
    3.times { Resque.push('restriction_normal', :class => 'OneHourRestrictionJob', :args => ['any args']) }
    Resque.size('restriction_normal').should == 3
    OneHourRestrictionJob.should_receive(:repush).exactly(3).times.and_return(true)
    Resque::Job.reserve('restriction_normal')
  end

  it "should set queue on restricted job class" do
    Resque.push('restriction_othernormal', :class => 'OneHourRestrictionJob', :args => ['any args'])
    Resque::Job.reserve('restriction_othernormal').should == Resque::Job.new('restriction_othernormal', {'class' => 'OneHourRestrictionJob', 'args' => ['any args']})
    OneHourRestrictionJob.source_queue.should == 'restriction_othernormal'
  end

  it "should set queue on still restricted job class" do
    Resque.redis.set(OneHourRestrictionJob.redis_key(:per_hour), -1)
    Resque.push('restriction_othernormal', :class => 'OneHourRestrictionJob', :args => ['any args'])
    Resque::Job.reserve('restriction_othernormal').should be_nil
    OneHourRestrictionJob.source_queue.should == 'restriction_othernormal'
  end

  it "should set queue on restricted job class" do
    Resque.push('othernormal', :class => 'OneHourRestrictionJob', :args => ['any args'])
    Resque::Job.reserve('othernormal').should == Resque::Job.new('othernormal', {'class' => 'OneHourRestrictionJob', 'args' => ['any args']})
    Resque::Job.reserve('othernormal').should be_nil
    OneHourRestrictionJob.source_queue.should == 'othernormal'
  end

  it "should not set queue on plain job class" do
    Resque.push('normal', :class => 'UnrestrictedJob', :args => ['any args'])
    UnrestrictedJob.should_not_receive(:queue=)
    Resque::Job.reserve('normal').should == Resque::Job.new('normal', {'class' => 'UnrestrictedJob', 'args' => ['any args']})
  end

end
