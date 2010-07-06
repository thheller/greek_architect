require File.dirname(__FILE__) + '/../spec_helper'


describe GreekArchitect::ColumnFamily do
  before(:each) do
    @client = GreekArchitect::Client.new('GreekTest', ['127.0.0.1:9160'])
    @row_config = GreekArchitect::RowConfig.new('User')
    @row_config.key_type = GreekArchitect::Types::UUIDv4.new()
    @column_family = @row_config.column_family(:profile)
    @column_family.compare_with = GreekArchitect::Types::String.new()
    @column_family.value_type = GreekArchitect::Types::MsgPack.new()
    
    @row = GreekArchitect::Row.new(@client, @row_config, nil)
  end

  context ".each" do
    it "should iterate over all columns" do
      
      profile = @row.column_family(:profile)
      profile.should be_an_instance_of(GreekArchitect::ColumnFamily)
      
      @row.mutate do
        profile.insert('test4', 1)
        profile.insert('test1', 1)
        profile.insert('test8', 1)
        profile.insert('test2', 1)
        profile.insert('test7', 1)
        profile.insert('test3', 1)
        profile.insert('test5', 1)
        profile.insert('test9', 1)
        profile.insert('test6', 1)
      end
      
      from_cassandra = profile.slice().collect { |col| col.name }
      names = ['test1', 'test2', 'test3', 'test4', 'test5', 'test6', 'test7', 'test8', 'test9']
      
      names.should == from_cassandra

      each_names = []
      profile.each(:batch_size => 2) do |col|
        each_names << col.name
      end
      
      each_names.should == names

      # default batch size is 100, which should work too
      each_names = []
      profile.each() do |col|
        each_names << col.name
      end
      
      each_names.should == names
      
      each_names = []
      profile.each(:start => 'test5', :batch_size => 3) do |col|
        each_names << col.name
      end
      
      each_names.should == names[4, 5]

      each_names = []
      profile.each(:finish => 'test5', :batch_size => 3) do |col|
        each_names << col.name
      end
      
      each_names.should == names[0, 5]
    end
  end
  
  context ".slice" do
    it "should return slices of the data" do

      profile = @row.column_family(:profile)
      profile.should be_an_instance_of(GreekArchitect::ColumnFamily)
      
      @row.mutate do
        profile.insert('test4', 1)
        profile.insert('test1', 1)
        profile.insert('test8', 1)
        profile.insert('test2', 1)
        profile.insert('test7', 1)
        profile.insert('test3', 1)
        profile.insert('test5', 1)
        profile.insert('test9', 1)
        profile.insert('test6', 1)
      end
      
      slice = profile.slice(:count => 5)
      slice.should be_an_instance_of(GreekArchitect::Slice)
      hash = slice.as_hash
      hash['test1'].should == 1
      hash['test2'].should == 1
      hash['test3'].should == 1
      hash['test4'].should == 1
      hash['test5'].should == 1
    end
  end
end