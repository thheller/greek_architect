module GreekArchitect
  class Hash < RowWrapper
    
    def self.inherited(klass)
      klass.extend(ColumnFamilyClassMethods)
      klass.extend(HashClassMethods)
    end
    
    def column_init!; super; @columns_loaded = []; @columns = {}; end
    attr_reader :columns, :columns_loaded
    
    def []=(column_name, column_value)
      if not col = columns[column_value]
        col = new_column_wrapper(column_name)
      end
      
      if column_value.nil?
        col.delete!
      else
        col.update(column_name, column_value)
      end
    end
    
    def [](column_name)
      if not col = columns[column_name]
        if not col = load_columns(column_name)
          return nil
        end
      end
      col.value
    end
    
    protected
    
    def new_column_wrapper(column_name)
      columns[column_name] ||= begin
        
        if not value_type = column_family["column:#{column_name}"]
          if not value_type = column_family["value_type"]
            raise "#{self.class} does not define a #{column_name} column and no default value_type is set"
          end
        end
        
        ColumnWrapper.new(self, column_family.compare_with, value_type)
      end
    end
    
    def load_columns(column_name)
      if not columns_loaded.include?(column_name)
        # columns_to_load = column_family['columns']
        
        predicate = CassandraThrift::SlicePredicate.new(
          # leave the optimization stuff for later, for now just load EVERY column!
          # :column_names => columns_to_load.collect { |it| column_family.compare_with.encode(it) }
          
          :slice_range => CassandraThrift::SliceRange.new(
            :start => '',
            :finish => '',
            :count => 1000 # FIXME: what if we have more than that? its a hash so you souldnt, but you may
          )
        )
      
        client.get_slice(column_family, key, column_parent, predicate, read_consistency_level).collect do |it|
          
          name = column_family.compare_with.decode(it.column.name)
        
          if not value_type = column_family["column:#{name}"]
            if not value_type = column_family["value_type"]
              raise "#{self.class} does not define a #{column_name} column and no default value_type is set"
            end
          end

          columns[name] = col = ColumnWrapper.new(self, column_family.compare_with, value_type)
          col.load(it.column)
          
          columns_loaded << name
        end
        
        # columns_loaded.concat(columns_to_load)
      end
      
      columns[column_name]
    end
  end
  
  module HashClassMethods
    def column(key, typename, opts = {})
      cf = GreekArchitect::column_family(self)
      
      define_method(key) do
        self[key]
      end
      
      define_method("#{key}=") do |v|
        self[key] = v
      end
      
      if gtype = GreekArchitect::greek_types.detect { |it| it.typename == typename }
        cf["column:#{key}"] = gtype.configure(opts)
        (cf["columns"] ||= []) << key
      else
        raise "#{self.class}.#{key} -> no such greek type: #{typename}"
      end
    end
  end
end