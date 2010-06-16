module GreekArchitect

  class RowWrapper
    def initialize(client, column_family, key)
      @client = client
      @column_family = column_family
      @key = key.nil? ? generate_key : key

      column_init!
    end
    
    # you might want better consistency when checking if an object exists
    def exists?(consistency_level = nil)
      column_count(consistency_level) > 0
    end
    
    alias_method :present?, :exists?
    
    def column_count(consistency_level = nil)
      client.get_count(column_family, key, column_parent, consistency_level || read_consistency_level)
    end
    
    def mutate(write_consistency_level = CassandraThrift::ConsistencyLevel::ONE, &block)
      client.mutate(write_consistency_level, block)
    end
    
    def delete_all!
      raise 'tbd'
    end

    attr_reader :column_family, :key
    
    def generate_key
      column_family.key.new_instance()
    end
    
    def id
      key
    end
    
    def read_consistency_level
      CassandraThrift::ConsistencyLevel::ONE
    end
      
    def client
      @client
    end
    
    def remove_column(col)
    end

    protected

    def column_init!; end

    def column_parent
      @column_parent ||= begin
        CassandraThrift::ColumnParent.new(:column_family => column_family.name)
      end
    end
  end
  
  module ColumnFamilyClassMethods
    def create()
      GreekArchitect::wrap(self, nil)
    end
    
    def get(key)
      GreekArchitect::wrap(self, key)
    end
    
    def value_type(typename, opts = {})
      cf = GreekArchitect::column_family(self)

      if gtype = GreekArchitect::greek_types.detect { |it| it.typename == typename }
        cf['value_type'] = gtype.configure(opts)
      else
        raise "no such greek type: #{typename}"
      end
    end    
    
    def override_name(new_name)
      cf = GreekArchitect::column_family(self)
      cf.name = new_name
    end
    
    def key(typename, opts = {})
      
      cf = GreekArchitect::column_family(self)
      
      if gtype = GreekArchitect::greek_types.detect { |it| it.typename == typename }
        cf.key = gtype.configure(opts)
      else
        raise ArgumentError, "#{self}.key no such greek type: #{typename}", caller
      end      
    end
    
    def compare_with(typename, opts = {})
      cf = GreekArchitect::column_family(self)
      
      if gtype = GreekArchitect::greek_types.detect { |it| it.typename == typename }
        cf.compare_with = gtype.configure(opts)
      else
        raise ArgumentError, "#{self}.compare_with no such greek type: #{typename}", caller
      end
    end  
  end
end
