require 'msgpack'

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