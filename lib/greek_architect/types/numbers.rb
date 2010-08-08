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
        raise ArgumentError, 'Long must be 8 bytes' unless bytes.length == 8
        
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