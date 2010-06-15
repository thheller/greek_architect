
require 'simple_uuid'

module GreekArchitect
  module Types
    
    class UUID < AbstractType
      register_as :uuid, 'org.apache.cassandra.db.marshal.TimeUUIDType'
      register_as :time_uuid, 'org.apache.cassandra.db.marshal.TimeUUIDType'
      
      def new_instance()
        SimpleUUID::UUID.new
      end
      
      def decode(bytes)
        SimpleUUID::UUID.new(bytes)
      end
      
      def encode(value)
        raise(ArgumentError, "value is of SimpleUUID type: #{value.class}", caller) unless value.is_a?(SimpleUUID::UUID)
        
        value.bytes
      end
    end
    
    class String < AbstractType
      register_as :string, 'org.apache.cassandra.db.marshal.UTF8Type'

      def decode(bytes)
        bytes
      end

      def encode(value)
        value
      end
    end
    
    class Symbol < AbstractType
      register_as :symbol, 'org.apache.cassandra.db.marshal.AsciiType'
      
      def decode(bytes)
        bytes.to_sym
      end

      def encode(value)
        value.to_s
      end      
    end
  end
end