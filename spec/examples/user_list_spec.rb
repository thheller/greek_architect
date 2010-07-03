require File.dirname(__FILE__) + '/../spec_helper'
require File.dirname(__FILE__) + '/user_model.rb'
require File.dirname(__FILE__) + '/user_list_model.rb'

describe User, "keep a userlist partitioned by date" do
  
  before(:each) do
    GreekArchitect::runtime.configure({
      'keyspace' => 'GreekArchitectByExample',
      'servers' => ['127.0.0.1:9160']
    })
    
    GreekArchitect::runtime.client.delete_everything!
  end
  
  def quick_create(username)
    user = User.create()
    user.profile[:name] = username
    user.profile[:created_at] = Time.now
  end

  it "example: how to access users by name" do
    today = Time.now
    
    test_names = [
      'Captain Hero', 'Foxxy Love', 'Ling-Ling',
      'Princess Clara', 'Spanky Ham', 'Toot Braunstein',
      'Wooldoor Sockbat', 'Xandir Wifflebottom'
    ]
    
    GreekArchitect::runtime.client.mutate do
      test_names.each do |it|
        quick_create(it)
      end
    end
    
    names = []
    ByCreationDate.walk_users_created_at(today) do |user|
      names << user.profile[:name]
    end
    
    names.should == test_names
  end
end