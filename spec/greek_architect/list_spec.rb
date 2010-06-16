require File.dirname(__FILE__) + '/../spec_helper'


# list<long, int>
class SimpleList < GreekArchitect::List
  override_name 'Long'

  key :string
  
  compare_with :long
  value_type :int
end

class SimpleUUID < GreekArchitect::List
  override_name 'TimeUUID'
  
  key :guid
  
  compare_with :time_uuid
  value_type :int
end

describe GreekArchitect::List do
  before(:each) do
    GreekArchitect::connect('GreekTest', '127.0.0.1:9160')
  end

  def row_key
    self.running_example.to_s
  end

  it "should keep a simple list of integers" do
    list = SimpleList.get(row_key)

    list.mutate do
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

  it "::TimeUUID should match cassandras sorting" do
    list = SimpleUUID.create()
    list.mutate do      
      list.append_value(1)
      list.append_value(2)
      list.append_value(3)
      list.append_value(4)
      list.append_value(5)
    end

    result = list.slice().collect { |col| col.value  }
    
    result.should == [1,2,3,4,5]
  end
end