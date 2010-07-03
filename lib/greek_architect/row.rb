

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
      klass.extend(RowHelperClassMethods)
      klass.extend(RowClassMethods)
    end
    
    # support for ActsAsRow
    def greek_architect_row
      self
    end
    
    def greek_architect_row_config
      @row_config
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
  
  module RowHelperClassMethods
    def column_family(name, compare_with = :symbol, value_type = :msgpack, &block)
      runtime = GreekArchitect::Runtime.instance
      
      cfg = runtime.get_row_config(self).column_family(name)
      cfg.compare_with = runtime.get_type_by_name(compare_with).configure()
      cfg.value_type = runtime.get_type_by_name(value_type).configure()

      block.call(cfg) if block_given?
      
      class_eval %{
        def #{name}
          @column_family_#{name} ||= begin
            ColumnFamily.new(greek_architect_row, greek_architect_row_config.column_family(#{name.inspect}))
          end
        end
      }
    end
    
    def on_mutation_of(row_klass, column_family, column_name, &block)
      runtime = GreekArchitect::Runtime.instance

      cfg = runtime.get_row_config(row_klass).column_family(column_family)
      cfg.register_observer(column_name, block)
    end
    
    # def list(name, compare_with, value_type, &block)
    #   column_family(name, compare_with, value_type, &block)
    # end
    # 
    # def hash(name, compare_with = :symbol, value_type = :msgpack, &block)
    #   column_family(name, compare_with, value_type, &block)
    # end    
  end
end