module GreekArchitect
  class Slice
    include ::Enumerable

    def initialize(column_family)
      @column_family = column_family
      @columns = []
    end
    
    attr_reader :column_family
    
    def append(name_raw, value_raw, timestamp)
      col = ColumnWrapper.new(@column_family.row, @column_family.config)
      col.load_raw_values(name_raw, value_raw, timestamp)
      @columns << col
      col
    end
    
    def each
      @columns.each do |col|
        yield(col)
      end
    end
    
    def [](name)
      if x = @columns.detect { |it| it.name == name }
        return x.value
      end
      
      nil
    end
    
    def names
      @columns.collect(&:name)
    end
    
    def values
      @columns.collect(&:value)
    end
    
    def as_hash
      result = {}
      @columns.each do |col|
        result[col.name] = col.value
      end
      result
    end

    def row; @column_family.row; end
    def first; @columns.first; end
    def last; @columns.last; end
    def length; @columns.length; end
  end
end