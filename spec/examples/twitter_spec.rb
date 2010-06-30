require 'spec_helper'
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
    
    user.tweet!('hello world!')
    
    list = fanboi.most_recent_tweets()
    list.length == 1
    first = list.first

    first.message[:body].should == 'hello world!'
    first.message[:created_by].should == user.key
  end
end