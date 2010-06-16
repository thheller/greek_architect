require 'spec_helper'
require File.dirname(__FILE__) + '/user_model.rb'


__END__

# TODO: this needs more work, thinking

class UserNameIndex < GreekArchitect::Hash # will create an GreekArchitect::UniqueIndex in the future
  key :string
  
  compare_with :symbol
  column :user_id, :uuid
  
  def user
    @user ||= User.get(user_id)
  end

  on_mutate(User, :name) do |user|
    idx = UserNameIndex.get(user.name)
    idx[:user_id] = user.key
  end
end

describe GreekArchitect::Hash do
  
  before(:each) do
    GreekArchitect::connect('GreekArchitectByExample', '127.0.0.1:9160')
  end

  it "example: how to access users by name" do
    zilence = User.create()
    zilence.should_not be_present
    
    zilence.mutate do
      zilence.name = 'zilence'
      zilence.created_at = Time.now
    end
    
    zilence.should be_present
    zilence.column_count.should == 2
  
    by_id = User.get(zilence.id)
    by_id.name.should == 'zilence'
    
    # IMPORTANT!
    # Bug 1: this is a UNIQUE index, that will overwrite without checking
    # Bug 2: this will not delete the old index if the name changes
    # consider these TODOs
    by_name = UserNameIndex.get('zilence')
    by_name.user.id.should == zilence.id
    by_name.user.name.should == 'zilence'
  end
end