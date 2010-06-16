
module GreekArchitect
  module Types
    
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