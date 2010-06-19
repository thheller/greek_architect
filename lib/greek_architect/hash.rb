module GreekArchitect
  class Hash < RowWrapper
    def self.inherited(klass)
      klass.extend(ColumnFamilyClassMethods)
      klass.extend(SugarClassMethods)
      klass.extend(HashClassMethods)
    end
    
    def []=(column_name, column_value)
      columns.insert(column_name, column_value)
    end
    
    def [](column_name)
      columns.get_value(column_name)
    end
  end
  
  module HashClassMethods
    def column(key, typename, opts = {})
      cf = GreekArchitect::column_family(self)
      
      if key.is_a?(Symbol)
        define_method(key) do
          self[key]
        end
      
        define_method("#{key}=") do |v|
          self[key] = v
        end
      end
      
      if gtype = GreekArchitect::greek_types.detect { |it| it.typename == typename }
        cf.named_columns[key] = gtype.configure(opts)
      else
        raise "#{self.class}.#{key} -> no such greek type: #{typename}"
      end
    end
  end
end