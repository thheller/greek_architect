
module GreekArchitect
  class TypeError < ArgumentError
    def initialize(expected, got)
      @expected = expected
      @got = got
    end
    
    attr_reader :expected, :got
    
    def inspect
      "<TypeError:#{object_id} @expected=#{@expected} @got=#{@got.inspect}>"
    end
    
    def message
      "TypeError expected=#{@expected} got=#{@got.inspect}"
    end
  end  
  
  module Types

    
    class AbstractType
      def initialize(opts = {})
        @opts = opts
      end
      
      def self.inherited(klass)
        klass.extend(ClassMethods)
      end
      
      def convert(value)
        value
      end

      def check_type!(expected, instance)
        if not instance.is_a?(expected)
          raise TypeError.new(expected, instance.class)
        end
      end
      
    end
    
    module ClassMethods
      def register_as(typename, fqcn = nil)
        GreekArchitect::Runtime.instance.register_type(typename, self, fqcn)
      end
    end
  end
end