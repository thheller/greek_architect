require File.dirname(__FILE__) + '/../../spec_helper'

describe GreekArchitect::Types::GUID do
  before(:each) do
    @type = GreekArchitect::Types::GUID.new()
  end

  it "should be random but unique" do
    @type.new_instance().version.should == 5
    
    # how the hell should I test that? :P
  end

end