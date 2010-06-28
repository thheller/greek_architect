

module GreekArchitect
  class NotConnected < StandardError; end
  
  class TypeInfo
    def initialize(typename, java_class, ruby_type)
      @typename = typename
      @java_class = java_class
      @ruby_type = ruby_type
    end
        
    attr_reader :typename, :java_class, :ruby_type
    
    def configure(opts = {})
      TypeInstance.new(self, ruby_type.new(opts))
    end
  end
  
  class TypeInstance
    def initialize(greek_type, wrapper)
      @greek_type = greek_type
      @wrapper = wrapper 
    end
    
    attr_reader :greek_type, :wrapper
    
    def new_instance()
      wrapper.new_instance()
    end
    
    def encode(value)
      raise "#{self.class} cannot encode nil values" if value.nil?
      
      wrapper.encode(value)
    end
    
    def decode(value)
      raise "#{self.class} cannot decode nil values" if value.nil?
      
      wrapper.decode(value)
    end
  end  
  
  class Runtime
    class << self
      def instance()
        @@runtime ||= Runtime.new()
      end
      
      protected :new
    end
    
    def initialize()
      @registered_types = {}
      @row_configs = {}
      @client = nil
    end
    
    def connect(keyspace, server)
      @client = Client.connect(server, keyspace)      
    end
    
    def client
      raise NotConnected, 'not connected' unless @client
      @client
    end
    
    def get_row_config(klass)
      raise 'not a class' unless klass.is_a?(Class)
      
      @row_configs[klass.to_s] ||= RowConfig.new(klass)
    end
    
    def get_type_by_name(typename)
      if not x = @registered_types[typename]
        raise ArgumentError, "no such greek type: #{typename}", caller
      end
      
      x
    end    
    
    def register_type(typename, ruby_type, java_class)
      @registered_types[typename] = TypeInfo.new(typename, java_class, ruby_type)
    end
  end
end