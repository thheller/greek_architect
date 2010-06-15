require File.dirname(__FILE__) + '/../../spec_helper'

describe GreekArchitect::Types::Long do
  before(:each) do
    @type = GreekArchitect::Types::Long.new()
  end
  
  it "decoding should only accept 8 byte values" do
    lambda {
      @type.decode('hello')
    }.should raise_error(ArgumentError)
  end
  
  it "should encode to 8 bytes" do
    bytes = @type.encode(1)
    bytes.length.should == 8
  end
end