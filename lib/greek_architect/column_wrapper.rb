module GreekArchitect
  
  class ColumnWrapper
    def initialize(row, column_family)
      @row = row
      @column_family = column_family
      
      @name = nil
      @value = nil
      
      @name_raw = nil
      @value_raw = nil
      
      @timestamp = nil
      
      @name_type = column_family.compare_with
    end
    
    def column_family; @column_family; end
    def row; @row; end
#   
#     
#     if tcol = @client.get(@row, column_name, consistency_level)
#       
#       col.load_raw_values(tcol.column.name, tcol.column.value, tcol.column.timestamp)
#       return col
#     end
#     
#     nil
    
    attr_reader :column_family
    
    def init_with_name(name)
      @name = name
    end
    
    def load_raw_values(name, value, timestamp)
      @name_raw = name
      @value_raw = value
      @timestamp = timestamp
      
      self
    end
    
    def create(name, value, timestamp = nil)
      @name = name
      @value = value
      @timestamp = timestamp
            
      parent.client.current_mutation.append_update(self)
      
      self
    end
    
    def delete!
      #predicate = CassandraThrift::SlicePredicate.new(
      #  :column_names => [@column.name]
      #)
      #
      #@parent.client.current_mutation.append(
      #  @parent.column_family,
      #  @parent.key,
      #  CassandraThrift::Mutation.new(
      #    :deletion => CassandraThrift::Deletion.new(
      #      :timestamp => @parent.client.timestamp,
      #      :predicate => predicate
      #    ))      
      #)
      
      @client.current_mutation.append_delete(self)
    end
    
    def set_value(value, timestamp = nil)
      @previous_value = self.value
      
      @value = value
      @timestamp = timestamp if timestamp

      @row.client.current_mutation.append_insert(self)
      
      self
    end
  
    def name
      raise 'no name supplied' unless (@name or @name_raw)
      @name ||= @name_type.decode(@name_raw)
    end
    
    def name_raw
      raise 'no name supplied' unless (@name or @name_raw)
      @name_raw ||= @name_type.encode(@name)
    end
    
    def value_type
      @value_type ||= begin
        v = (@column_family.named_columns[name] || @column_family.value_type)
        raise "could not find proper value type for #{@column_family.name}" unless v
        v
      end
    end
  
    def value
      @value ||= (@value_raw.nil? ? nil : value_type.decode(@value_raw))
    end
    
    def value_raw
      @value_raw ||= (@value.nil? ? nil : value_type.encode(@value))
    end
    
    def previous_value
      @previous_value
    end
    
    def timestamp
      @timestamp
    end
    
    def inspect
      "<GreekArchitect::ColumnWrapper:#{object_id} @row=#{row.inspect} @name=#{name.inspect} @value=#{value.inspect}>"
    end
  end
end