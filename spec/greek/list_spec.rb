require File.dirname(__FILE__) + '/../spec_helper'

# list<long, int>
class SimpleList < Greek::List
  key :string
  
  compare_with :long
  value_type :int
end

describe Greek::List do
  before(:each) do
    @client = Greek::Client.connect('127.0.0.1:9160', 'GreekTest')
    # Greek::inspect()
  end

  def row_key
    self.running_example.to_s
  end

  it "should keep a simple list of integers" do
    list = @client.wrap(SimpleList, row_key)

    @client.mutate() do
      list.insert(2, 10)
      list.insert(1, 5)
      list.insert(3, 15)
    end
    
    # inserted values should be sorted by cassandra
    
    slice = list.slice(:count => 2)
    slice.length.should == 2
    
    slice[0].name.should == 1
    slice[0].value.should == 5
    
    slice[1].name.should == 2
    slice[1].value.should == 10

    slice = list.slice(:start => 2, :count => 1)
    slice.length.should == 1
    
    slice[0].name.should == 2
    slice[0].value.should == 10
  end

end