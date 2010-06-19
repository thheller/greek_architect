module GreekArchitect
  
  class ColumnFamily
    def initialize(name)
      @name = name
      @named_columns = {}
      @observers = {}
    end
        
    attr_reader :ruby_type, :named_columns
    attr_accessor :compare_with, :name, :key, :value_type
    
    def register_observer(column_name, callback)
      (@observers[column_name] ||= []) << callback
    end
    
    def each_observer(column_name)
      if list = @observers[column_name]
        list.each do |callback|
          yield(callback)
        end
      end
    end
  end
end