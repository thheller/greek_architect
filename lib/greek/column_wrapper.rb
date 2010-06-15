module Greek
  
  class ColumnWrapper
    def initialize(parent, name_type, value_type)
      @parent = parent
      @name_type = name_type
      @value_type = value_type
    end
    
    attr_reader :parent, :name_type, :value_type
    
    def load(col)
      @column = col
      
      self
    end
    
    def new_column(name, value, timestamp)
      @column = CassandraThrift::Column.new(
        :name => name_type.encode(name),
        :value => value_type.encode(value),
        :timestamp => timestamp || parent.client.timestamp
      )      
    end
    
    def create(name, value, timestamp = nil)
      raise(ArgumentError, "already have a column?") if not @column.nil?
      
      new_column(name, value, timestamp)
      
      @parent.client.current_mutation.append(@parent.column_family, @parent.key, CassandraThrift::Mutation.new(
        :column_or_supercolumn => CassandraThrift::ColumnOrSuperColumn.new(
          :column => @column
        )
      ))    
      
      self
    end
    
    def delete!
      predicate = CassandraThrift::SlicePredicate.new(
        :column_names => [@column.name]
      )
      
      @parent.client.current_mutation.append(
        @parent.column_family,
        @parent.key,
        CassandraThrift::Mutation.new(
          :deletion => CassandraThrift::Deletion.new(
            :timestamp => @parent.client.timestamp,
            :predicate => predicate
          ))      
      )
      
      @parent.remove_column(self)
    end
    
    def update(name, value, timestamp = nil)
      @value = value
      if @column.nil?
        new_column(name, value, timestamp)        
      else
        @column.value = @value_type.encode(value)
        @column.timestamp = timestamp || @parent.client.timestamp
      end

      @parent.client.current_mutation.append(@parent.column_family, @parent.key, CassandraThrift::Mutation.new(
        :column_or_supercolumn => CassandraThrift::ColumnOrSuperColumn.new(
          :column => @column
        )
      ))
    end
  
    def name
      @name ||= @name_type.decode(@column.name)
    end
    
    def name_raw
      @column.name
    end
  
    def value
      @value ||= @value_type.decode(@column.value)
    end
    
    def value_raw
      @column.value
    end
  
    def timestamp
      @timestamp ||= Time.at(@column.timestamp / 1000000)
    end
    
    def timestamp_raw
      @column.timestamp
    end
    
    def inspect
      "\#Greek::ColumnWrapper:#{object_id} @name=#{name.inspect} @value=#{value.inspect}>"
    end
  end
end