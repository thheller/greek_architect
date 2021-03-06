# Copyright (c) 2010 Thomas Heller
# 
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
# 
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


module GreekArchitect
  module Types
    
    module UUIDConverter
      def convert(value)
        if value.is_a?(::UUID)
          return value
        elsif value.is_a?(::String) and value =~ /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i
          return ::UUID.parse(value)
        else
          raise TypeError.new(::UUID, value)
        end
      end

      def incr(value)
        value.succ
      end      
      
      def decode(bytes)
        ::UUID.from_raw_bytes(bytes)
      end

      def encode(value)
        return value if value.is_a?(::String) and value == ''
        
        check_type!(::UUID, value)
        value.raw_bytes
      end
    end
    
    class TimeUUID < AbstractType
      include UUIDConverter
      register_as :time_uuid, 'org.apache.cassandra.db.marshal.TimeUUIDType'

      def new_instance()
        ::UUID.create_v1_faster()
      end
    end 
    
    # aka random
    class UUIDv4 < AbstractType
      include UUIDConverter
      register_as :uuid_v4, 'org.apache.cassandra.db.marshal.LexicalUUIDType'

      def new_instance()
        ::UUID.create_v4
      end
    end
    
    class GUID < AbstractType
      include UUIDConverter

      register_as :uuid, 'org.apache.cassandra.db.marshal.LexicalUUIDType'
      register_as :guid, 'org.apache.cassandra.db.marshal.LexicalUUIDType'
      
      def initialize(opts = {})
        super(opts)
        
        @ns = UUID::create_v1_faster
      end
      
      def new_instance()
        # severly paranoid about conflicts (2 random numbers in the same usec should be pretty unique)
        ::UUID.create_sha1([Time.now.to_f, rand(), rand()].join(""), @ns)
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