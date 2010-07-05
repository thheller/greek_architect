require File.dirname(__FILE__) + '/../spec_helper'


class TestRow < GreekArchitect::Row
  key :uuid
  
  column_family(:simple_hash, :symbol, :msgpack)
  column_family(:time_list, :time_uuid, :msgpack)
  column_family(:long_list, :long, :msgpack)
end

describe TestRow do
  
  before(:each) do
    GreekArchitect::runtime.configure({
      'keyspace' => 'GreekTest',
      'servers' => ['127.0.0.1:9160']
    })
  end
  
  def create_row()
    TestRow.create() do |row|
      row.simple_hash[:test1] = 'value1'
      row.simple_hash[:test2] = 'value2'
      row.simple_hash[:test3] = 'value3'
      
      row.time_list.append_value('value1')
      row.time_list.append_value('value2')
      row.time_list.append_value('value3')
      
      row.long_list.insert(1, 'value1')
      row.long_list.insert(2, 'value2')
      row.long_list.insert(3, 'value3')
    end
  end
  
  it "should not hit cassandra when accessing a column by name we just wrote" do
    row = create_row()
    
    row.client.should_not_receive(:thrift_call)
    row.simple_hash[:test1].should == 'value1'
    row.simple_hash[:test2].should == 'value2'
    row.simple_hash[:test3].should == 'value3'
    
    row.long_list[1].should == 'value1'
    row.long_list[2].should == 'value2'
    row.long_list[3].should == 'value3'
  end
  
  it "should delete columns by name when setting its value to nil" do
    row = create_row()
    
    row.simple_hash[:test1].should == 'value1'
    row.mutate do
      row.simple_hash[:test1] = nil
      row.simple_hash[:test2] = nil
      row.simple_hash[:test3] = nil
    end
    
    row.simple_hash[:test1].should == nil
    row.simple_hash.slice().length == 0
  end
end