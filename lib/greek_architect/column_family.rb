module GreekArchitect
  
  class ColumnFamily
    def initialize(ruby_type)
      @ruby_type = ruby_type
      
      if ruby_type.to_s =~ /(.+)::(.+)/
        @name = $2
      else
        @name = ruby_type.to_s
      end
      
      @config = {}
    end
        
    attr_reader :ruby_type
    attr_accessor :compare_with, :name, :key
    
    def []=(k, v)
      @config[k] = v
    end
    
    def [](k)
      @config[k]
    end
  end
end