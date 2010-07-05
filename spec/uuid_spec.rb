require File.dirname(__FILE__) + '/spec_helper'
require 'uuid'

describe ::UUID do
  it "should generate unique v1 uuids faster" do
    list = []
    
    1000.times { list << ::UUID.create_v1_faster }
    
    list.collect { |it| it.to_s }.uniq.length == 1000
    
    # well thats kinda pointless to test
    # one single process cannot create duplicates :P
  end
  
  it "should reconstruct timestamps" do
    time = Time.now
    u1 = ::UUID.create_v1_faster(time)
    time.should == u1.timestamp   
  end
  
  it "should create v1 successors properly" do
    u1 = ::UUID.create_v1_faster
    u2 = u1.succ
    
    u1.timestamp.should < u2.timestamp
  end
  
  it "should create v4 successors properly" do
    u1 = ::UUID.create_v4
    u2 = u1.succ
    
    u1.should < u2
  end
  
  it "should extract mac_addrs for v1" do
    u1 = ::UUID.create_v1_faster
    u1.mac_addr.should == UUID.pseudo_mac_addr
    
    u2 = ::UUID.create_v4
    u2.mac_addr.should be_nil
  end
end