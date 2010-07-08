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
  
  it "should get multiple row slices with ranges" do
    row1 = create_row()
    row2 = create_row()
    row3 = create_row()
    
    list = TestRow.multiget_slice([row1.key, row2.key, row3.key], :simple_hash, :start => :test2, :count => 1)
  
    GreekArchitect::runtime.client.should_not_receive(:thrift_call)
  
    list.length.should == 3
    list[0].row.key.should == row1.key
    list[1].row.key.should == row2.key
    list[2].row.key.should == row3.key
    
    list.each do |x|
      x.should be_an_instance_of(GreekArchitect::Slice)
      x.row.should be_an_instance_of(TestRow)
      x.length.should == 1
      x[:test2].should == 'value2'
    end
  end
  
  it "should get multiple row slices with names" do
    row1 = create_row()
    row2 = create_row()
    row3 = create_row()
    
    list = TestRow.multiget_slice([row1.key, row2.key, row3.key], :simple_hash, :names => [:test1, :test3])
  
    GreekArchitect::runtime.client.should_not_receive(:thrift_call)
  
    list.length.should == 3
    list[0].row.key.should == row1.key
    list[1].row.key.should == row2.key
    list[2].row.key.should == row3.key
    
    list.each do |x|
      x.should be_an_instance_of(GreekArchitect::Slice)
      x.row.should be_an_instance_of(TestRow)
      x.length.should == 2
      x.names.should == [:test1, :test3]
      x[:test1].should == 'value1'
      x[:test2].should == nil
      x[:test3].should == 'value3'
    end
  end

  it "should be able to iterate over all rows" do
    GreekArchitect::runtime.client.delete_everything!
    
    value = 0
    100.times do
      TestRow.create() do |row|
        row.simple_hash[:value] = (value += 1)
      end
    end
    
    count = 0
    keys = []
    values = []
    
    TestRow.each_slice(:simple_hash, :names => [:value]) do |slice|
      count += 1
      
      keys << slice.row.key.to_s
      values << slice[:value]
    end
    
    keys.uniq.length.should == 100
    values.uniq.length.should == 100
     
    keys.each do |key|
      TestRow.remove!(key, :simple_hash)
    end
  end
end