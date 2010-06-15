module GreekArchitect
  module Types
    
    # 8 byte [sec , usec] 4 byte each
    class Timestamp < AbstractType
      register_as :timestamp
      
      def decode(bytes)
        Time.at(*bytes.unpack('N2'))
      end

      def encode(value)
        [value.to_i, value.usec].pack('N2')
      end      
    end
  end
end