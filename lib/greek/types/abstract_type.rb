
module Greek
  module Types
    class AbstractType
      def initialize(opts = {})
        @opts = opts
      end
      
      def self.inherited(klass)
        klass.extend(ClassMethods)
      end
      
      def check_type!(expected, instance)
        if not instance.is_a?(expected)
          raise(ArgumentError, "value is not of type #{expected}: #{instance.class}", caller)
        end
      end
      
    end
    
    module ClassMethods
      def register_as(typename, fqcn = nil)
        Greek.register_type(typename, self, fqcn)
      end
    end
  end
end