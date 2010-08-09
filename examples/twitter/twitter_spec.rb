require File.dirname(__FILE__) + '/../../spec/spec_helper'
require File.dirname(__FILE__) + '/twitter_model.rb'

describe 'Twitter' do
  before(:each) do
    GreekArchitect::runtime.configure({
      'keyspace' => 'TwitterSpec',
      'servers' => ['127.0.0.1:9160']
    })
  end
  
  it "should work" do
        
    user = User.create()
    user.mutate do
      user.profile[:name] = 'zilence'
      user.profile[:created_at] = Time.now
    end
    
    user.profile.should be_present
    
    fanboi = User.create()
    fanboi.mutate do
      fanboi.profile[:name] = 'fanboi'
      fanboi.profile[:created_at] = Time.now
    end

    fanboi.profile.should be_present
    
    fanboi.follow!(user)
    
    ff = fanboi.following.slice()
    ff.length.should == 1
    ff.first.name.should == user.key
    
    uf = user.followers.slice()
    uf.length.should == 1
    uf.first.name.should == fanboi.key
    
    tweet1 = user.tweet!('hello world! 1')
    tweet2 = user.tweet!('hello world! 2')
    tweet3 = user.tweet!('hello world! 3')
    tweet4 = user.tweet!('hello world! 4')
    tweet5 = user.tweet!('hello world! 5')
    tweet6 = user.tweet!('hello world! 6')
    tweet7 = user.tweet!('hello world! 7')
    tweet8 = user.tweet!('hello world! 8')
    tweet9 = user.tweet!('hello world! 9')
    
    ut = user.timeline.slice(:count => 100, :reversed => true)
    ut.length == 9
    
    ut.first.value.should == tweet9.key
    
    list = fanboi.timeline_tweets(:start => tweet4.key, :count => 10)
    list.length.should == 3
    
    list[0][:body].should == 'hello world! 3'
    list[1][:body].should == 'hello world! 2'
    list[2][:body].should == 'hello world! 1'
  end
end