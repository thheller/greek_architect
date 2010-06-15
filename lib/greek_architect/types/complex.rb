require 'msgpack'

module GreekArchitect
  module Types

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