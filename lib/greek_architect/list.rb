module GreekArchitect
  class List < RowWrapper
    
    def self.inherited(klass)
      klass.extend(ColumnFamilyClassMethods)
      klass.extend(ListClassMethods)
    end
    
    def value_type
      @value_type ||= GreekArchitect::column_family(self.class)['value_type']
    end
    
    def append_value(value)
      wrapper = ColumnWrapper.new(self, column_family.compare_with, value_type)
      wrapper.create(column_family.compare_with.new_instance(), value)
    end

    def get(name)
      column_path = CassandraThrift::ColumnPath.new(
        :column_family => column_family.name,
        :column => column_family.compare_with.encode(name)
      )
      
      result = client.get(column_family, key, column_path, read_consistency_level)

      if result
        col = ColumnWrapper.new(self, column_family.compare_with, value_type)
        col.load(result.column)
        col
      end
    end

    def slice(opts = {})      
      start = if x = opts[:start]
        column_family.compare_with.encode(x)
      else
        ''
      end
      
      finish = if x = opts[:finish]
        column_family.compare_with.encode(x)
      else
        ''
      end
      
      predicate = CassandraThrift::SlicePredicate.new(
        :slice_range => CassandraThrift::SliceRange.new(
          :start => start,
          :finish => finish,
          :reversed => opts[:reversed] || false,
          :count => opts[:count] || 100
        )
      )
      
      client.get_slice(column_family, key, column_parent, predicate, read_consistency_level).collect do |it|
        ColumnWrapper.new(self, column_family.compare_with, value_type).load(it.column)
      end
    end

    def insert(name, value, timestamp = nil)
      wrapper = ColumnWrapper.new(self, column_family.compare_with, value_type)
      wrapper.create(name, value, timestamp)
    end
  end
  
  module ListClassMethods
  end
end