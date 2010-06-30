require File.dirname(__FILE__) + '/../spec_helper'


describe GreekArchitect::Client do
  before(:each) do
    @client = GreekArchitect::Client.new('TwitterSpec', ['127.0.0.1:9160'])
  end
  
  it "should return nil when a column is not found" do    
    row_config = GreekArchitect::RowConfig.new('User')
    column_family = row_config.column_family(:profile)
    column_family.compare_with = GreekArchitect::Types::String.new()
    
    row = GreekArchitect::Row.new(@client, row_config, '123')
    
    column = @client.get(row, column_family, 'test')
    column.should be_nil
  end
end