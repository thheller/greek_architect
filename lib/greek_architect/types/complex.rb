require 'msgpack'
require 'json'

module GreekArchitect
  module Types
    
    module UUIDConverter
      def decode(bytes)
        ::UUID.from_raw_bytes(bytes)
      end

      def encode(value)
        check_type!(::UUID, value)
        value.raw_bytes
      end
    end
    
    class TimeUUID < AbstractType
      include UUIDConverter
      register_as :time_uuid, 'org.apache.cassandra.db.marshal.TimeUUIDType'

      def new_instance()
        ::UUID.create_v1
      end
    end 
    
    # aka random
    # TODO: figure out if lexical requires a 'string' version instead of raw bytes
    class UUIDv4 < AbstractType
      include UUIDConverter

      register_as :uuid, 'org.apache.cassandra.db.marshal.LexicalUUIDType'
      register_as :uuid_v4, 'org.apache.cassandra.db.marshal.LexicalUUIDType'
      register_as :guid, 'org.apache.cassandra.db.marshal.LexicalUUIDType'

      def new_instance()
        ::UUID.create_v4
      end
    end
    
    # FIXME: has to resort to an ugly hack to be able to encode
    # Simple values like  'asdf',1 , 1.1, etc
    # since the JSON.generate only otherwise would accept Hash or Array object
    class JSON < AbstractType
      register_as :json
      
      # uses a slight hack
      def decode(bytes)
        ::JSON.parse(bytes)['value']
      end
      
      def encode(value)
        ::JSON.generate({'value' => value})
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