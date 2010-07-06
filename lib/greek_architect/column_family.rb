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
    
    attr_reader :row, :config
    
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
    
    def new_slice()
      s = Slice.new(self)
    end

    def slice(opts = {})      
      s = new_slice()
      
      @row.client.get_slice(@row, @config, opts).each do |it|
        s.append(it.column.name, it.column.value, it.column.timestamp)
      end
      
      s
    end
    
    def last_timestamp
      list = slice(:reversed => true, :count => 1)
      list.empty? ? -1 : list[0].timestamp
    end
    
    # walk in batches since thrift doesnt support streaming and transfering 100.000 cols at once kinda sux
    def each(opts = {})
      current_start = opts[:start] || ''
      finish = opts[:finish] || ''
      batch_size = opts[:batch_size] || 100
      
      while list = slice(:start => current_start, :finish => finish, :count => batch_size)
        list.each do |it|
          yield(it)
        end
        
        # batch wasnt full, so we are done
        break if list.length < batch_size
        
        current_start = @config.compare_with.incr(list.last.name)
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
    
    def delete(column_name)
      col = column_wrapper(column_name)
      col.delete!
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