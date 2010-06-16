require 'spec_helper'
require File.dirname(__FILE__) + '/user_model.rb'

describe GreekArchitect::Hash do
  
  before(:each) do
    GreekArchitect::connect('GreekArchitectByExample', '127.0.0.1:9160')
  end

  it "example: how to access users by name" do
    # BEWARE: HARDCORE DETAILS
    # THIS COULD BE AUTOMATED AT SOME POINT, BUT I REALLY DO NOT LIKE MAGIC!

    # Step1: create our name index
    class UserNameIndex < GreekArchitect::Hash # will create an GreekArchitect::UniqueIndex in the future
      key :string
      
      compare_with :symbol
      column :user_id, :uuid
    
      def user
        User.get(user_id)
      end
    end
  
    # Step2: write the index when writing the name
    class User
      alias_method :really_set_name, :name=

      def name=(value)
        idx = client.wrap(UserNameIndex, value)
        really_set_name(value)
        idx[:user_id] = self.key
      end
    end
  
    zilence = User.create()
    zilence.should_not be_present
    
    zilence.mutate do
      zilence.name = 'zilence'
      zilence.created_at = Time.now
    end
    
    zilence.should be_present
    zilence.count.should == 2
  
    by_id = User.get(zilence.id)
    by_id.name.should == 'zilence'
    
    # the index basically looks like "zilence" => { :user_id => guid_of_zilence }

    # IMPORTANT!
    # Bug 1: this is a UNIQUE index, that will overwrite without checking
    # Bug 2: this will not delete the old index if the name changes
    # consider these TODOs
    by_name = UserNameIndex.get('zilence')
    by_name.user.name.should == 'zilence'
  end
end