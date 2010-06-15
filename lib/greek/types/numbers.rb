
module Greek
  module Types
    # org.apache.cassandra.db.marshal.AsciiType
    # org.apache.cassandra.db.marshal.BytesType
    # org.apache.cassandra.db.marshal.LongType
    # org.apache.cassandra.db.marshal.LexicalUUIDType
    # org.apache.cassandra.db.marshal.LongType
    # org.apache.cassandra.db.marshal.TimeUUIDType
    # org.apache.cassandra.db.marshal.UTF8Type
    
    # 8 byte
    class Long < AbstractType
      register_as :long, 'org.apache.cassandra.db.marshal.LongType'
      
      def decode(bytes)
        raise ArgumentError, 'Long should be 8 bytes' unless bytes.length == 8
        
        bytes.unpack('Q').first
      end

      def encode(value)
        if value == nil
          ''
        else
          [value].pack('Q')
        end
      end      
    end
    
    # 4 byte    
    class Integer < AbstractType
      register_as :int

      def decode(bytes)
        bytes.unpack('N').first
      end

      def encode(value)
        [value].pack('N')
      end      
    end
    
    # '1' => 1
    class StringInt < AbstractType
      register_as :int_string
      
      def decode(bytes)
        bytes.to_i
      end

      def encode(value)
        value.to_s
      end
    end    
  end
end