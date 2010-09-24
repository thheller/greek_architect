module GreekArchitect
  
  class ThriftAdapter
    def initialize(client, server)
      @client = client      
      @server = server
      
      @successful_calls = 0
    end
    
    def handle_error(err)
      puts "#{self.inspect} -> Caught #{err.class} #{err.message}, continuing"
      disconnect!
    end
    
    def success!
      @successful_calls += 1
    end
    
    def disconnect!
      @transport.close if @transport
      @socket.close if @socket
      @successful_calls = 0
    end
    
    def connect!
      if not open?
        host, port = @server.split(/:/)

        @socket = Thrift::Socket.new(host, port)
        @transport = Thrift::BufferedTransport.new(@socket)
        
        @transport.open

        @protocol = Thrift::BinaryProtocol.new(@transport)

        @thrift = CassandraThrift::Cassandra::Client.new(@protocol)        
      end
    end
    
    def open?
      @socket and @socket.open? and @transport and @transport.open?
    end
    
    def thrift
      raise 'not open' unless open?
      
      @thrift
    end
    
    def inspect
      "<ThriftAdapter:#{object_id} @server=#{@server.inspect} @sucessful_calls=#{@successful_calls}>"
    end
  end
end