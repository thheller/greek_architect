require 'spec_helper'
require File.dirname(__FILE__) + '/user_model.rb'
require File.dirname(__FILE__) + '/user_name_index_model.rb'
require File.dirname(__FILE__) + '/user_list_index_model.rb'

describe GreekArchitect::Hash, "keep a userlist partitioned by date" do
  
  before(:each) do
    @client = GreekArchitect::connect('GreekArchitectByExample', '127.0.0.1:9160')
    
    @client.mutate do
      User.delete_all_rows!
      UserNameIndex.delete_all_rows!
      UserListIndex.delete_all_rows!
    end
  end
  
  def quick_create(username)
    user = User.create()
    user.name = username
    user.created_at = Time.now
  end

  it "example: how to access users by name" do
    today = Time.now
    
    test_names = [
      'Captain Hero', 'Foxxy Love', 'Ling-Ling',
      'Princess Clara', 'Spanky Ham', 'Toot Braunstein',
      'Wooldoor Sockbat', 'Xandir Wifflebottom'
    ]
    
    @client.mutate do
      test_names.each do |it|
        quick_create(it)
      end
    end
    
    names = []
    UserListIndex.each_user_created_at(today) do |user|
      names << user.name
    end
    
    names.should == test_names
    
    lingling = UserNameIndex.get_user('Ling-Ling')
    lingling.should be_present
    lingling.name.should == 'Ling-Ling'
  end
end