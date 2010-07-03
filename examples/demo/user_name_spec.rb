require File.dirname(__FILE__) + '/../../spec/spec_helper'
require File.dirname(__FILE__) + '/user_model.rb'
require File.dirname(__FILE__) + '/user_name_model.rb'

describe User, "access by username" do
  
  before(:each) do
    GreekArchitect::runtime.configure({
      'keyspace' => 'GreekArchitectByExample',
      'servers' => ['127.0.0.1:9160']
    })
  end

  it "example: how to access users by name" do
    zilence = User.create()
    zilence.profile.should_not be_present

    # FIXME: this is a UNIQUE index, I'm not NOT checking if the name was taken and will just overwrite!!!
    zilence.mutate do
      zilence.profile[:name] = 'zilence'
      zilence.profile[:created_at] = Time.now
    end
    
    zilence.profile.should be_present
    zilence.profile.column_count.should == 2
  
    by_id = User.get(zilence.key)
    by_id.profile[:name].should == 'zilence'
    
    by_name = ByName.get_user('zilence')
    by_name.profile.should be_present
    
    by_name.id.should == zilence.id
    by_name.profile[:name].should == 'zilence'
    
    # now lets rename
    zilence.mutate do
      zilence.profile[:name] = 'renamed!'
    end
    
    by_name = ByName.get_user('renamed!')
    by_name.profile.should be_present
    by_name.id.should == zilence.id
    by_name.profile[:name].should == 'renamed!'
    
    zilence.profile[:name].should == 'renamed!'
    
    user = User.get(zilence.key)
    user.profile[:name].should == 'renamed!'
    

    by_name = ByName.get_user('zilence')
    by_name.should be_nil
  end
end