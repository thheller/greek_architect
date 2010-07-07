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
  class Spy
    def initialize(delegate)
      @delegate = delegate
    end
    
    def method_missing(method, *args)
      start = Time.now.to_f
      puts "THRIFT: ----> #{method} #{args.inspect}"
      res = @delegate.send(method, *args)
      puts "THRIFT: <- #{"%.5f" % (Time.now.to_f - start)}ms - #{res.inspect}"
      res
    end
  end

  
  class Client
    def initialize(keyspace, servers)
      @servers = servers

      connect_to_server!
      
      @keyspace = keyspace
      @current_mutation = nil
    end
    
    def info(klass)
      schema[klass.to_s]
    end
    
    def schema()
      @schema ||= begin
        thrift_call { |t| t.describe_keyspace(@keyspace) }
      end
    end   
    
    def read_consistency_level
      :one
    end    
    
    def translate_consistency_level(value)
      CassandraThrift::ConsistencyLevel::ONE
    end
    
    def get(row, column_family, column_name, consistency_level = nil)    
      column_path = CassandraThrift::ColumnPath.new(
        :column_family => column_family.cassandra_name,
        :column => column_family.compare_with.encode(column_name)
      )
      
      thrift_call do |t|
        begin
          t.get(@keyspace, row.key.to_s, column_path, translate_consistency_level(consistency_level || read_consistency_level))
        rescue CassandraThrift::NotFoundException
          nil
        end
      end
    end
    
    def get_count(row, column_family, consistency_level)
      column_parent = CassandraThrift::ColumnParent.new(:column_family => column_family.cassandra_name)
      
      thrift_call do |t|
        t.get_count(@keyspace, row.key.to_s, column_parent,
            translate_consistency_level(consistency_level || read_consistency_level))
      end
    end
    
    def multiget_slice(keys, column_family, opts)
      consistency_level = translate_consistency_level(opts[:consistency] || read_consistency_level)
      
      column_parent = CassandraThrift::ColumnParent.new(
        :column_family => column_family.cassandra_name
      )
      
      predicate = CassandraThrift::SlicePredicate.new()
      
      if names = opts[:names]
        predicate.column_names = names.collect { |it| column_family.compare_with.encode(it) }
      else
        start = if x = opts[:start]
          column_family.compare_with.encode(x)
        else
          ''
        end
      
        finish = if x = opts[:finish]
          column_family.compare_with.encode(x)
        else
          ''
        end

        predicate.slice_range = CassandraThrift::SliceRange.new(
            :start => start,
            :finish => finish,
            :reversed => opts[:reversed] || false,
            :count => opts[:count] || 100 # TODO: whats a reasonable default here?
        )
      end
      
      thrift_call do |t|
        t.multiget_slice(@keyspace, keys.collect(&:to_s), column_parent, predicate, consistency_level)
      end
    end
  
    def get_slice(row, column_family, opts)
      start = if x = opts[:start]
        column_family.compare_with.encode(x)
      else
        ''
      end
      
      finish = if x = opts[:finish]
        column_family.compare_with.encode(x)
      else
        ''
      end
      
      consistency_level = translate_consistency_level(opts[:consistency] || read_consistency_level)
      
      column_parent = CassandraThrift::ColumnParent.new(
        :column_family => column_family.cassandra_name
      )
      
      predicate = CassandraThrift::SlicePredicate.new(
        :slice_range => CassandraThrift::SliceRange.new(
          :start => start,
          :finish => finish,
          :reversed => opts[:reversed] || false,
          :count => opts[:count] || 100 # TODO: whats a reasonable default here?
        )
      )
      
      thrift_call do |t|
        t.get_slice(@keyspace, row.key.to_s, column_parent, predicate, consistency_level)
      end
    end
    
    def batch_mutate(mutations, consistency_level)
      mutation_map = generate_mutation_map(mutations)
      thrift_call do |t|
        t.batch_mutate(@keyspace, mutation_map, translate_consistency_level(consistency_level))
      end
    end
    
    def generate_mutation_map(mutations)
      mutation_map = {}

      mutations.each do |mutation|
        x = mutation_map[mutation.column.row.key.to_s] ||= {}
        y = x[mutation.column.column_family.cassandra_name] ||= []

        thrift_mutation = case mutation.action
        when :insert
          CassandraThrift::Mutation.new(
            :column_or_supercolumn => CassandraThrift::ColumnOrSuperColumn.new(
              :column => CassandraThrift::Column.new(
                :name => mutation.column.name_raw,
                :value => mutation.column.value_raw,
                :timestamp => mutation.column.timestamp || timestamp
              )
            )
          )
        when :delete
          CassandraThrift::Mutation.new(
            :deletion => CassandraThrift::Deletion.new(
              :predicate => CassandraThrift::SlicePredicate.new(
                :column_names => [mutation.column.name_raw]
                ),
              :timestamp => mutation.column.timestamp || timestamp
            )
          )
        else
          raise "dunno how to do #{mutation.inspect}"
        end

        y << thrift_mutation
      end

      mutation_map
    end
    
    def delete_everything!
      schema().keys.each do |cf_name|
            
         column_parent = CassandraThrift::ColumnParent.new(
           :column_family => cf_name
         )
         
         column_path = CassandraThrift::ColumnPath.new(
           :column_family => cf_name
         )         
     
         predicate = CassandraThrift::SlicePredicate.new(
           :slice_range => CassandraThrift::SliceRange.new(
             :start => '',
             :finish => '',
             :count => 1
           )
         )
     
         key_range = CassandraThrift::KeyRange.new(
           :start_key => '',
           :end_key => '',
           :count => 10000
           # FIXME: this is really bad!
         )
        
        
        thrift_call do |t|
          t.get_range_slices(@keyspace, column_parent, predicate, key_range, CassandraThrift::ConsistencyLevel::ONE).each do |it|

            unless it.columns.empty?
              t.remove(@keyspace, it.key, column_path, timestamp, CassandraThrift::ConsistencyLevel::ONE)
            end
          end
        end        
      end
    end

    def wrap_row(row_config, key)
      key = row_config.key_type.convert(key)
      Row.new(self, row_config, key)
    end
    
    def wrap(klass, key = nil)
      row_config = GreekArchitect::runtime.get_row_config(klass)      
      key = row_config.key_type.convert(key)
      klass.new(self, row_config, key)
    end

    def mutate(consistency_level = nil, &block)
      raise AlreadyMutating, 'already mutating' unless @current_mutation.nil?

      @current_mutation = Mutation.new(self, consistency_level || CassandraThrift::ConsistencyLevel::ONE)
      begin
        block.call()
        
        @current_mutation.execute!
      ensure
        @current_mutation = nil
      end
    end
    
    def current_mutation
      raise NotMutating, 'not currently mutating' if @current_mutation.nil?
      
      @current_mutation 
    end
    
    def timestamp
      (Time.new.to_f * 1_000_000).to_i
    end
    
    def disconnect!
      @transport.close
      @socket.close
    end
    
    def spy!
      return if @thrift.is_a?(Spy)
      
      @orig_thrift = @thrift
      @thrift = Spy.new(@thrift)
    end
    
    def unspy!
      @thrift = @orig_thrift
    end
    
    protected
    
    def connect_to_server!
      @current_server = @servers[rand(@servers.length)]
      host, port = @current_server.split(/:/)
      
      @socket = Thrift::Socket.new(host, port)
      @transport = Thrift::BufferedTransport.new(@socket)

      if not @transport.open
        raise 'connection failed'
      end      

      @protocol = Thrift::BinaryProtocol.new(@transport)

      @thrift = CassandraThrift::Cassandra::Client.new(@protocol)
    end
    
    def thrift_call 
      begin        
        connect_to_server! if not (@socket and @socket.open?)
        
        yield(@thrift)
      
      rescue Thrift::TransportException => err
        @socket.close
        puts "caught Thrift::TransportException, retrying"
        retry
        
      rescue
        puts "THRIFT_INTERCEPT! #{$!.class} - #{$!.message}"
        raise
      end
    end    
  end
end