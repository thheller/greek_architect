

module GreekArchitect
  class RowConfig
    def initialize(name)
      @name = name
      @key_type = nil
      @column_families = {}
    end
    
    def cassandra_name
      @name
    end
    
    def column_family(name)
      @column_families[name] ||= ColumnFamilyConfig.new(self, name)
    end
    
    attr_accessor :key_type
  end
  
  class Row
    def self.inherited(klass)
      klass.extend(RowClassMethods)
    end
    
    def initialize(client, row_config, key)
      @row_config = row_config

      @client = client
      @key = key || @row_config.key_type.new_instance()
    end
    
    def key; @key; end
    def id; @key; end
    
    attr_reader :client
    
    def mutate(write_consistency_level = :one)
      @client.mutate(write_consistency_level) do
        yield()
      end
    end
    
    def inspect
      "<#{self.class}:#{object_id} key=#{key.inspect}>"
    end
  end
  
  module RowClassMethods
    def key(type_name)
      runtime = GreekArchitect::Runtime.instance
      
      cfg = runtime.get_row_config(self)
      cfg.key_type = runtime.get_type_by_name(type_name).configure()
    end
    
    def column_family(name, compare_with, value_type, &block)
      runtime = GreekArchitect::Runtime.instance
      
      cfg = runtime.get_row_config(self).column_family(name)
      cfg.compare_with = runtime.get_type_by_name(compare_with).configure()
      cfg.value_type = runtime.get_type_by_name(value_type).configure()

      block.call(cfg) if block_given?
      
      class_eval %{
        def #{name}
          @column_family_#{name} ||= begin
            ColumnFamily.new(self, @row_config.column_family(#{name.inspect}))
          end
        end
      }
    end
    
    def list(name, compare_with, value_type, &block)
      column_family(name, compare_with, value_type, &block)
    end
    
    def hash(name, &block)
      column_family(name, :symbol, :msgpack, &block)
    end
    
    def create()
      x = GreekArchitect::Runtime.instance.client.wrap(self, nil)
      if block_given?
        x.mutate do
          yield(x)
        end
      end
      
      x
    end
    
    def get(key)
      x = GreekArchitect::Runtime.instance.client.wrap(self, key)
      if block_given?
        x.mutate do
          yield(x)
        end
      end
      
      x
    end
  end
end