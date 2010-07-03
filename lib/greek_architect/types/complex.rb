require 'msgpack'
require 'json'
require 'uuidtools'

module GreekArchitect
  module Types
    
    module UUIDConverter
      def convert(value)
        if value.is_a?(UUIDTools::UUID)
          return value
        elsif value.is_a?(::String) and value =~ /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i
          return UUIDTools::UUID.parse(value)
        else
          raise TypeError.new(UUIDTools::UUID, value)
        end
      end
      
      def decode(bytes)
        ::UUIDTools::UUID.parse_raw(bytes)
      end

      def encode(value)
        check_type!(::UUIDTools::UUID, value)
        value.raw
      end
    end
    
    class TimeUUID < AbstractType
      include UUIDConverter
      register_as :time_uuid, 'org.apache.cassandra.db.marshal.TimeUUIDType'

      def min_value()
        ::UUIDTools::UUID.timestamp_create(Time.at(0))
      end

      # FIXME: precision? 
      def incr(value)
        timestamp = value.timestamp
        ::UUIDTools::UUID.timestamp_create(timestamp + 0.00001)
      end
      
      def new_instance()
        ::UUIDTools::UUID.timestamp_create()
      end
    end 
    
    # aka random
    class UUIDv4 < AbstractType
      include UUIDConverter

      register_as :uuid, 'org.apache.cassandra.db.marshal.LexicalUUIDType'
      register_as :uuid_v4, 'org.apache.cassandra.db.marshal.LexicalUUIDType'
      register_as :guid, 'org.apache.cassandra.db.marshal.LexicalUUIDType'

      def new_instance()
        ::UUIDTools::UUID.random_create()
      end
    end
    
    class JSON < AbstractType
      register_as :json
      
      def decode(bytes)
        ::JSON.parse(bytes)
      end
      
      def encode(value)
        ::JSON.generate(value)
      end
    end

    class MsgPack < AbstractType
      register_as :msgpack

      def decode(bytes)
        MessagePack.unpack(bytes)
      end

      def encode(value)
        value.to_msgpack
      end
    end
  end
end