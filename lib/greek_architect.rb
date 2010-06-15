$LOAD_PATH << File.expand_path(File.dirname(__FILE__))

require 'rubygems'
require 'thrift_client'
require 'greek_architect/gen-rb/cassandra_constants'
require 'greek_architect/gen-rb/cassandra_types'
require 'greek_architect/gen-rb/cassandra'

module GreekArchitect
  class AlreadyMutating < StandardError
  end
  
  class NotMutating < StandardError
  end

  class Type
    def initialize(typename, java_class, ruby_type)
      @typename = typename
      @java_class = java_class
      @ruby_type = ruby_type
    end
        
    attr_reader :typename, :java_class, :ruby_type
    
    def configure(opts)
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

  
  class << self
    
    def inspect()
      puts '--- GREEK COLUMN FAMILIES'
      pp @@column_family

      puts '--- GREEK TYPES'
      pp @@greek_types
    end
    
    def column_family(type)
      (@@column_family ||= {})[type] ||= begin
        ColumnFamily.new(type)
      end
    end
    
    def register_type(typename, ruby_type, java_class)
      (@@greek_types ||= []) << Type.new(typename, java_class, ruby_type)
    end
    
    def greek_types()
      (@@greek_types ||= [])
    end
  end
end

require 'greek_architect/client'
require 'greek_architect/column_family'
require 'greek_architect/row_wrapper'
require 'greek_architect/column_wrapper'
require 'greek_architect/mutation'
require 'greek_architect/types/abstract_type'
require 'greek_architect/types/numbers'
require 'greek_architect/types/strings'
require 'greek_architect/types/dates'
require 'greek_architect/types/complex'
require 'greek_architect/list'
require 'greek_architect/hash'


