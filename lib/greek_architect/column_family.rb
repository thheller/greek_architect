module GreekArchitect

  class ColumnFamilyConfig
    def initialize(row_config, name)
      @row_config = row_config
      @name = name
      @named_columns = {}
      @observers = {}
    end

    def cassandra_name
      @cassandra_name ||= [@row_config.cassandra_name, @name].collect { |it| it.to_s.capitalize }.join("")
    end

    def column(name, type_name)
      @named_columns[name] = GreekArchitect::runtime.get_type_by_name(type_name).configure()
    end

    attr_reader :named_columns
    attr_accessor :compare_with, :name, :value_type

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

  class ColumnFamily
    def initialize(row, config)
      @row = row
      @config = config
      @columns = {}
    end
    
    def inspect
      "<ColumnFamily:#{object_id}-#{@config.cassandra_name} @columns=#{@columns.inspect}>"
    end

    # you might want better consistency when checking if an object exists
    def exists?(consistency_level = nil)
      # suggested by stuhood on #cassandra
      # if theres any column in this row we declare it as existant
      # faster than doing a column_count() > 0
      slice(:count => 1).any?
    end

    alias_method :present?, :exists?

    def column_count(consistency_level = nil)
      @row.client.get_count(column_family, @row.key, consistency_level)
    end    

    def slice(opts = {})      
      @row.client.get_slice(@row, @config, opts).collect do |it|
        col = ColumnWrapper.new(@row, @config)
        col.load_raw_values(it.column.name, it.column.value, it.column.value)
        @columns[col.name] = col
        col
      end
    end
    
    def each
      slice(:count => 5).each do |col|
        yield(col)
      end
    end

    def insert(column_name, column_value, timestamp = nil)
      col = column_wrapper(column_name)

      if column_value.nil?
        col.delete!
      else
        col.set_value(column_value, timestamp)
      end      
    end

    def [](column_name)
      get(column_name).value
    end
    
    def []=(column_name, column_value)
      get(column_name).set_value(column_value)
    end

    def get_value(column_name, consistency_level = nil)
      if x = get(column_name, consistency_level)
        return x.value
      end
    end
    
    def append_value(value, timestamp = nil)
      get(@config.compare_with.new_instance()).set_value(value)
    end

    def get(column_name, consistency_level = nil)
      column_wrapper(column_name)
    end
 

    def column_wrapper(column_name)
      @columns[column_name] ||= begin
        value_type = @config.named_columns[column_name] || @config.value_type

        col = ColumnWrapper.new(@row, @config)
        col.init_with_name(column_name)
        col
      end
    end    
    #
    #def method_missing(method, *args)
    #  ms = method.to_s
    #
    #  if ms[-1, 1] == '=' and args.length == 1
    #    column_name = ms[0, ms.length-1].to_sym
    #    if @config.named_columns[column_name]
    #      return get(column_name).set_value(args[0])
    #    end
    #  else
    #  end
    #
    #  super
    #end
  end
end