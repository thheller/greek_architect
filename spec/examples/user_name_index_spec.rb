require 'spec_helper'
require File.dirname(__FILE__) + '/user_model.rb'
require File.dirname(__FILE__) + '/user_name_index_model.rb'

describe GreekArchitect::Hash, "access by username" do
  
  before(:each) do
    GreekArchitect::connect('GreekArchitectByExample', '127.0.0.1:9160')
  end

  it "example: how to access users by name" do
    zilence = User.create()
    zilence.should_not be_present

    # FIXME: this is a UNIQUE index, I'm not NOT checking if the name was taken and will just overwrite!!!
    zilence.mutate do
      zilence.name = 'zilence'
      zilence.created_at = Time.now
    end
    
    zilence.should be_present
    zilence.column_count.should == 2
  
    by_id = User.get(zilence.id)
    by_id.name.should == 'zilence'
    
    by_name = UserNameIndex.get_user('zilence')
    by_name.should be_present
    
    by_name.id.should == zilence.id
    by_name.name.should == 'zilence'
    
    # now lets rename
    zilence.mutate do
      zilence.name = 'renamed!'
    end
    
    by_name = UserNameIndex.get_user('renamed!')
    by_name.should be_present

    by_name.id.should == zilence.id
    by_name.name.should == 'renamed!'
    
    zilence.name.should == 'renamed!'
    
    user = User.get(zilence.key)
    user.name.should == 'renamed!'
    
    begin
      by_name = UserNameIndex.get_user('zilence')
    rescue CassandraThrift::NotFoundException
    end
    
    # apparently thrift disconnects on an exception and pretends there are no more servers?
    # so every query after this will fail unless I reconnect manually
    # WTF?
  end
end