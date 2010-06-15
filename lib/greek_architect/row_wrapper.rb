
module GreekArchitect
  class RowWrapper
    
    def initialize(client, column_family, key)
      @client = client
      @column_family = column_family
      @key = key || generate_key

      column_init!
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
