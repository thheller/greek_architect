require 'spec_helper'
require File.dirname(__FILE__) + '/user_model.rb'

describe GreekArchitect::Hash do
  
  before(:each) do
    GreekArchitect::connect('GreekArchitectByExample', '127.0.0.1:9160')
  end

  it "should be simple to use" do
    
    user = User.create()
    # the key is ALWAYS generated, user.id is never nil!
    user.id.should be_an_instance_of(::UUID)
    user.key.should be_an_instance_of(::UUID)
    
    # id is just an alias for key ...
    user.id.should == user.key
    
    # begin our mutation
    user.mutate do
      # named column sugar
      user.name = 'zilence'
      user.created_at = Time.now
      
      # set the value for an unnamed column
      # its messagepack'd so throw some random values in there
      user[:some_array] = [1,2,3,4,5,true,false,{ "o'rly" => "yarly" },"no wai"]
      # not I usually do not build arrays like that ;)
      
      # FIXME: would like to use symbol keys in those hashes to
      # but msgpack/json do not differentiate between symbols/string so we are at their mercy
      # we may insert symbols, but will get strings back, so I dont
      user[:some_hash] = { 'hello' => 'world' }
      user[:other_hash] = { 'wicked' => true }
      user[:some_string] = 'hi there'
      user[:some_int] = 1337
      user[:some_float] = 3.1337
    end

    # mutation finished, our data is now saved, lets see
    user.should be_present # aka user.present? / user.exists?
    user.column_count.should == 8 # we created 8 columns
    
    # note that every modification MUST happen inside a mutate block!
    lambda {
      user.name = 'fail'
    }.should raise_error(GreekArchitect::NotMutating)

    
    # we could continue using that object
    # but we want to read it back from cassandra    
    zilence = User.get(user.id)
    zilence.name.should == 'zilence'
    zilence.created_at.should == user.created_at

    zilence[:some_array].should == [1,2,3,4,5,true,false,{ "o'rly" => "yarly" },"no wai"]
    zilence[:some_hash].should == { 'hello' => 'world' }
    zilence[:other_hash].should == { 'wicked' => true }
    zilence[:some_string].should == 'hi there'
    zilence[:some_int].should == 1337
    zilence[:some_float].should == 3.1337
    
    # we can also access the columns directly (also includes timestamp)
    # we even get access to the timestamp values (even if hidden a little)

    zilence.columns[:name].value.should == 'zilence'
    zilence.columns[:name].name.should == :name
    zilence.columns[:name].timestamp.should > 0
    
    # if you want the raw values (as they are stored in cassandra aka msgpack binaries)
    # zilence.columns[:some_array].value_raw
    # zilence.columns[:some_array].name_raw
    # zilence.columns[:some_array].timestamp_raw    
  end
end