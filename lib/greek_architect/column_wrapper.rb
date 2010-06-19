module GreekArchitect
  
  class ColumnWrapper
    def initialize(client, row)
      @client = client
      @row = row
      
      @name = nil
      @value = nil
      
      @name_raw = nil
      @value_raw = nil
      @timestamp = nil
      
      @name_type = row.column_family.compare_with
    end
    
    def column_family
      @row.column_family
    end
    
    attr_reader :client, :row
    
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
    
    def insert(value, timestamp = nil)
      @previous_value = self.value
      
      @value = value
      @value_raw = value_type.encode(value)
      
      @timestamp = timestamp if timestamp

      @client.current_mutation.append_insert(self)

      
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
        v = (row.column_family.named_columns[name] || row.column_family.value_type)
        raise "could not find proper value type for #{row.column_family.name}" unless v
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
      "<GreekArchitect::ColumnWrapper:#{object_id} @row=#{row.key.to_s.inspect} @name=#{name.inspect} @value=#{value.inspect}>"
    end
  end
end