# Copyright (c) 2010 Thomas Heller
# 
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
# 
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

module GreekArchitect
  
  class ColumnWrapper
    def initialize(row, column_family)
      @row = row
      @column_family = column_family
      
      @name = nil
      @value = nil
      
      @name_raw = nil
      @value_raw = nil
      
      @timestamp = 0
      
      @name_type = column_family.compare_with
    end
    
    def column_family; @column_family; end
    def row; @row; end
    
    def load_column()
      if tcol = @row.client.get(@row, @column_family, name)
        load_raw_values(tcol.column.name, tcol.column.value, tcol.column.timestamp)
      end
    end
    
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
      assign_value(value)

      @timestamp = timestamp || @row.client.timestamp
            
      parent.client.current_mutation.append_update(self)
      
      self
    end
    
    def delete!
      @previous_value = @value
      @value = nil
      @timestamp = @row.client.timestamp

      @row.client.current_mutation.append_delete(self)
    end
    
    def set_value(value, timestamp = nil)
      assign_value(value)
      
      @timestamp = timestamp || @row.client.timestamp

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
      @value ||= begin
        if @value_raw.nil?
          load_column()
        end
        
        if @value_raw.nil?
          nil
        else
          value_type.decode(@value_raw)
        end
      end
    end
    
    def value_raw
      @value_raw
    end
    
    def previous_value
      @previous_value
    end
    
    def timestamp
      @timestamp
    end
    
    def inspect
      "<GreekArchitect::ColumnWrapper:#{object_id} @row=#{row.inspect} @name=#{name.inspect} @value=#{value.inspect} @timestamp=#{@timestamp.inspect}>"
    end
  
    private

    def assign_value(value)
      @previous_value = @value if @value
      @value = value

      begin
        @value_raw = value_type.encode(value)
      rescue TypeError => err
        raise "Failed to encode value for #{@row.class}.#{@column_family.name} column: #{err.message}"
      end
    end
  end
end