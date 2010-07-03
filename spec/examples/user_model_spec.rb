require File.dirname(__FILE__) + '/../spec_helper'
require File.dirname(__FILE__) + '/user_model.rb'

describe User do
  
  before(:each) do
    GreekArchitect::runtime.configure({
      'keyspace' => 'GreekArchitectByExample',
      'servers' => ['127.0.0.1:9160']
    })
  end

  it "should be simple to use" do
    
    user = User.create()
    # the key is ALWAYS generated, user.id is never nil!
    user.id.should be_an_instance_of(UUIDTools::UUID)
    user.key.should be_an_instance_of(UUIDTools::UUID)
    
    # id is just an alias for key ...
    user.id.should == user.key
    
    # begin our mutation
    user.mutate do
      # named column sugar
      user.profile[:name] = 'zilence'
      user.profile[:created_at] = Time.now
      
      # set the value for an unnamed column
      # its messagepack'd so throw some random values in there
      user.profile[:some_array] = [1,2,3,4,5,true,false,{ "o'rly" => "yarly" },"no wai"]
      # not I usually do not build arrays like that ;)
      
      # FIXME: would like to use symbol keys in those hashes to
      # but msgpack/json do not differentiate between symbols/string so we are at their mercy
      # we may insert symbols, but will get strings back, so I dont
      user.profile[:some_hash] = { 'hello' => 'world' }
      user.profile[:other_hash] = { 'wicked' => true }
      user.profile[:some_string] = 'hi there'
      user.profile[:some_int] = 1337
      user.profile[:some_float] = 3.1337
    end

    # mutation finished, our data is now saved, lets see

    user.profile.should be_present # aka user.present? / user.exists?
    user.profile.column_count.should == 8 # we created 8 columns
    
    # note that every modification MUST happen inside a mutate block!
    lambda {
      user.profile[:name] = 'fail'
    }.should raise_error(GreekArchitect::NotMutating)

    
    # we could continue using that object
    # but we want to read it back from cassandra    
    zilence = User.get(user.id)
    zilence.profile[:name].should == 'zilence'
    zilence.profile[:created_at].should == user.profile[:created_at]

    zilence.profile[:some_array].should == [1,2,3,4,5,true,false,{ "o'rly" => "yarly" },"no wai"]
    zilence.profile[:some_hash].should == { 'hello' => 'world' }
    zilence.profile[:other_hash].should == { 'wicked' => true }
    zilence.profile[:some_string].should == 'hi there'
    zilence.profile[:some_int].should == 1337
    zilence.profile[:some_float].should == 3.1337
    
    # we can also access the columns directly (also includes timestamp)
    # we even get access to the timestamp values (even if hidden a little)

    col = zilence.profile.get(:name)
    col.value.should == 'zilence'
    col.name.should == :name
    col.timestamp.should > 0
    
    # if you want the raw values (as they are stored in cassandra aka msgpack binaries)
    # zilence.columns[:some_array].value_raw
    # zilence.columns[:some_array].name_raw
    # zilence.columns[:some_array].timestamp_raw    
  end
end