module GreekArchitect

  class ColumnFamilyConfig
    def initialize(row_config, name)
      @row_config = row_config
      @name = name
      @named_columns = {}
      @observers = {}
    end

    def cassandra_name
      @cassandra_name ||= [@row_config.cassandra_name, @name.to_s.split("_").collect { |it| it.to_s.capitalize }.join("")].join("_")
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
      @row.client.get_slice(@row, @config, {:count => 1}).any?
    end

    alias_method :present?, :exists?

    def column_count(consistency_level = nil)
      @row.client.get_count(@row, @config, consistency_level)
    end
    
    def hash_slice(opts = {})
      result = {}
      slice(opts).each do |col|
        result[col.name] = col.value
      end
      result
    end

    def slice(opts = {})      
      @row.client.get_slice(@row, @config, opts).collect do |it|
        col = ColumnWrapper.new(@row, @config)
        col.load_raw_values(it.column.name, it.column.value, it.column.timestamp)
        @columns[col.name] = col
        col
      end
    end
    
    def last_timestamp
      list = slice(:reversed => true, :count => 1)
      list.empty? ? -1 : list[0].timestamp
    end
    
    def each
      current_start = @config.compare_with.min_value
      batch_size = 5
      
      while (list = slice(:start => current_start, :count => batch_size)) and not list.empty?
        list.each do |it|
          yield(it)
          
          current_start = @config.compare_with.incr(it.name)
        end
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