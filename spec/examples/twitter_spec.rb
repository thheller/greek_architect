require File.dirname(__FILE__) + '/../spec_helper.rb'
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
    
    tweet = user.tweet!('hello world!')
    
    ut = user.timeline.slice()
    ut.length == 1
    ut.first.value.should == tweet.key
    
    list = fanboi.most_recent_tweets()
    list.length.should == 1
    
    first = list.first
    first.message[:body].should == 'hello world!'
    first.message[:created_by].should == user.key
  end
end