require File.dirname(__FILE__) + '/../spec_helper'

class SimpleHash < GreekArchitect::Hash
  override_name 'Ascii'

  key :string
  
  compare_with :symbol

  column :created_at, :timestamp
  column :name, :string
  column :empty, :string

end

describe GreekArchitect::Hash do
  
  before(:each) do
    @client = GreekArchitect::Client.connect('127.0.0.1:9160', 'GreekTest')
    
    GreekArchitect::inspect()
  end

  def row_key
    self.running_example.to_s
  end
  
  it "should fail when mutating twice" do
    @client.mutate do
      lambda {
        @client.mutate
      }.should raise_error(GreekArchitect::AlreadyMutating)
    end
  end
  
  it "should not allow updates when not mutating" do
    hash = @client.wrap(SimpleHash, row_key)
    lambda {
      hash.name = 'zilence'
    }.should raise_error(GreekArchitect::NotMutating)
  end
  
  it "decide if this is a problem" do
    hash = @client.wrap(SimpleHash, row_key)
    
    @client.mutate do
      hash.name = 'zilence'
      hash.name = 'test'

      # so what happens here is that we append ONE mutation to change the column name to 'zilence'
      # then we add another to set it to 'test'
      # what happens internally is that the first mutation will be changed to be exaclty the same
      # as the second one since they both keep a reference to thrift column and update it
      # since I have no idea how cassandra handles this I'm just gonna assume its no biggy
      # since the end result is what we want anyways but we really should remove the first
      # mutation since its pointless to insert one value and immediantly overwrite it
      
      # AKA I'M LAZY
    end
    
    other_hash = @client.wrap(SimpleHash, row_key)
    hash.name.should == 'test'
  end
    
  it "should be able to access the raw column as well as sugar methods/hash accessors" do
    hash = @client.wrap(SimpleHash, row_key)
    hash.id.should == row_key
    
    @client.mutate do
      hash.name = 'zilence'
      hash.created_at = Time.now
    end
    
    # read into another object            
    other_hash = @client.wrap(SimpleHash, row_key)
    other_hash.id.should == row_key
    other_hash[:name].should == 'zilence'
    other_hash.name.should == 'zilence'
    
    other_hash.columns[:name].timestamp.should_not be_nil
    other_hash.columns[:name].timestamp.should be_an_instance_of(Time)
    other_hash.columns[:name].timestamp_raw.should > 1
    
    other_hash.columns[:name].name.should == :name
    other_hash.columns[:name].name_raw.should == 'name' # ruby should have a ByteArray type, but strings are fine
    
    other_hash.columns[:name].value.should == 'zilence'
    other_hash.columns[:name].value_raw.should == 'zilence'
    
    other_hash[:created_at].should == hash[:created_at]
    other_hash.created_at.should == hash[:created_at]
    other_hash.created_at.should be_an_instance_of(Time)

    other_hash[:empty].should == nil
    other_hash.empty.should == nil
    
    @client.mutate do
      hash.name = nil # aka delete
    end
    
    another = @client.wrap(SimpleHash, row_key)
    another.name.should be_nil
    
  end
  
end