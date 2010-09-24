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
  class RowConfig
    def initialize(name)
      @name = name
      @key_type = nil
      @column_families = {}
    end
    
    def cassandra_name
      @name
    end
    
    def column_family(name)
      @column_families[name] ||= ColumnFamilyConfig.new(self, name)
    end
    
    attr_accessor :key_type
  end
  
  class Row
    def self.inherited(klass)
      klass.extend(RowHelperClassMethods)
      klass.extend(RowClassMethods)
    end
    
    # support for ActsAsRow
    def greek_architect_row
      self
    end
    
    def greek_architect_row_config
      @row_config
    end
    
    def initialize(client, row_config, key)
      @row_config = row_config

      @client = client
      @key = key || @row_config.key_type.new_instance()
      @column_family_instances = {}
    end
    
    def column_family(name)
      @column_family_instances[name] ||= begin
        ColumnFamily.new(greek_architect_row, greek_architect_row_config.column_family(name))
      end      
    end
    
    def key; @key; end
    def id; @key; end
    
    attr_reader :client
    
    def mutate(write_consistency_level = :one)
      @client.mutate(write_consistency_level) do
        yield()
      end
    end
    
    def inspect
      "<#{self.class}:#{object_id} key=#{key.inspect}>"
    end
  end
  
  module RowClassMethods
    def key(type_name)
      runtime = GreekArchitect::Runtime.instance
      
      cfg = runtime.get_row_config(self)
      cfg.key_type = runtime.get_type_by_name(type_name).configure()
    end
    
    def create()
      x = GreekArchitect::Runtime.instance.client.wrap(self, nil)
      if block_given?
        x.mutate do
          yield(x)
        end
      end
      
      x
    end
    
    def get(key)
      x = GreekArchitect::Runtime.instance.client.wrap(self, key)
      if block_given?
        x.mutate do
          yield(x)
        end
      end
      
      x
    end
    
    def remove!(key, column_family)
      runtime = GreekArchitect::Runtime.instance
      client = runtime.client
      
      row_config = runtime.get_row_config(self)
      cf_config = row_config.column_family(column_family)

      client.remove(key, cf_config)
    end
    
    def multiget_slice(keys, column_family, slice_opts = {})
      runtime = GreekArchitect::Runtime.instance
      client = runtime.client
      
      row_config = runtime.get_row_config(self)
      cf_config = row_config.column_family(column_family)
      
      result = {}
      map = client.multiget_slice(keys, cf_config, slice_opts)
      map.each do |k, cols|
        slice = get(k).column_family(column_family).new_slice()
        cols.each do |col|
          slice.append(col.column.name, col.column.value, col.column.timestamp)
        end
        
        result[slice.row.key] = slice
      end
      
      keys.collect { |it| result[it] }
    end
    
    def each_slice(column_family, slice_opts = {})
      runtime = GreekArchitect::Runtime.instance
      client = runtime.client
      
      row_config = runtime.get_row_config(self)
      cf_config = row_config.column_family(column_family)
      
      path_to_java = File.expand_path(File.dirname(__FILE__) + '/../../java')
      tkn_convert = IO.popen("/usr/bin/env java -cp #{path_to_java} KeyToToken", "r+")
      
      split_size = 5000
      batch_size = 1000
      
      # basically do what the hadoop/pig stuff does
      # 
      token_ranges = client.describe_ring()      
      token_ranges.each do |token_range|

        splits = client.describe_splits(token_range.start_token, token_range.end_token, split_size)

        x = 0
        while x < splits.length - 1
          start_token = splits[x]
          end_token = splits[x+1]
          x += 1
          
          while true
            list = client.get_range_slices(cf_config, start_token, end_token, batch_size, slice_opts)

            list.each do |thrift_slice|
              if not thrift_slice.columns.empty?
                slice = get(thrift_slice.key).column_family(column_family).new_slice()
                thrift_slice.columns.each do |col|
                  slice.append(col.column.name, col.column.value, col.column.timestamp)
                end   
              
                yield(slice)
              end
            end
            
            break if list.length < batch_size           
            
            start_token = token_for_key(tkn_convert, list.last.key)
          end
        end
      end
      
      tkn_convert.close
    end
    
    def token_for_key(tkn_convert, key)
      tkn_convert.puts(key)
      tkn_convert.gets().strip
      
      # d = Digest::MD5.new()
      # d.update(key)
      # bytes = d.digest().unpack('C*')
      # bytes.inject { |r, i| (r << 8) | i }.abs
      
      # produces different tokens than the java version
      # must be something wrong but I'm a math noob and cant figure it out
    end
  end
  
  module RowHelperClassMethods
    def column_family(name, compare_with = :symbol, value_type = :msgpack, &block)
      runtime = GreekArchitect::Runtime.instance
      
      cfg = runtime.get_row_config(self).column_family(name)
      cfg.compare_with = runtime.get_type_by_name(compare_with).configure()
      cfg.value_type = runtime.get_type_by_name(value_type).configure()

      block.call(cfg) if block_given?
      
      class_eval %{
        def #{name}
          @column_family_#{name} ||= begin
            ColumnFamily.new(greek_architect_row, greek_architect_row_config.column_family(#{name.inspect}))
          end
        end
      }
    end
    
    def on_mutation_of(row_klass, column_family, column_name, &block)
      runtime = GreekArchitect::Runtime.instance

      cfg = runtime.get_row_config(row_klass).column_family(column_family)
      cfg.register_observer(column_name, block)
    end
    
    # def list(name, compare_with, value_type, &block)
    #   column_family(name, compare_with, value_type, &block)
    # end
    # 
    # def hash(name, compare_with = :symbol, value_type = :msgpack, &block)
    #   column_family(name, compare_with, value_type, &block)
    # end    
  end
end
