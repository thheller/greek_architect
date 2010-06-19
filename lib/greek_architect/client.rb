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
      puts "-------------------> Thrift.#{method}"
      pp args
      
      res = @delegate.send(method, *args)
      puts "<-------------------- took: #{Time.now.to_f - start}ms"
      res
    end
  end
  
  class Client
    
    def self.connect(server, keyspace)
      thrift = ThriftClient.new(
        CassandraThrift::Cassandra::Client,
        [server],
        { :transport_wrapper => Thrift::BufferedTransport })
      
      new(thrift, keyspace)
    end
    
    def initialize(thrift, keyspace)
      @thrift = thrift
      # @thrift = Spy.new(@thrift)
      @keyspace = keyspace
      @current_mutation = nil
    end
    
    def info(klass)
      schema[klass.to_s]
    end
    
    def schema()
      @schema ||= @thrift.describe_keyspace(@keyspace)
    end   
    
    def read_consistency_level
      CassandraThrift::ConsistencyLevel::ONE
    end    
    
    def get(row, column_name, consistency_level)    
      # for 0.7 we should use column_family.key.encode(key) so we use binary keys
      column_path = CassandraThrift::ColumnPath.new(
        :column_family => row.column_family.name,
        :column => row.column_family.compare_with.encode(column_name)
      )
      
      @thrift.get(@keyspace, row.key.to_s, column_path, consistency_level || read_consistency_level)
    end
    
    def get_count(column_family, key, consistency_level)
      @thrift.get_count(
        @keyspace,
        key.to_s,
        CassandraThrift::ColumnParent.new(:column_family => column_family.name),
        consistency_level || read_consistency_level)
    end
  
    def get_slice(row, opts)
      start = if x = opts[:start]
        row.column_family.compare_with.encode(x)
      else
        ''
      end
      
      finish = if x = opts[:finish]
        row.column_family.compare_with.encode(x)
      else
        ''
      end
      
      consistency_level = opts[:consistency] || read_consistency_level
      
      column_parent = CassandraThrift::ColumnParent.new(
        :column_family => row.column_family.name
      )
      
      predicate = CassandraThrift::SlicePredicate.new(
        :slice_range => CassandraThrift::SliceRange.new(
          :start => start,
          :finish => finish,
          :reversed => opts[:reversed] || false,
          :count => opts[:count] || 1000 # TODO: whats a reasonable default here?
        )
      )
      
      @thrift.get_slice(@keyspace, row.key.to_s, column_parent, predicate, consistency_level).collect do |it|
        ColumnWrapper.new(self, row).load_raw_values(it.column.name, it.column.value, it.column.value)
      end
    end
    
    def batch_mutate(mutations, consistency_level)
      mutation_map = generate_mutation_map(mutations)
      @thrift.batch_mutate(@keyspace, mutation_map, consistency_level)
    end
    
    def generate_mutation_map(mutations)
      mutation_map = {}

      mutations.each do |mutation|
        x = mutation_map[mutation.column.row.key.to_s] ||= {}
        y = x[mutation.column.row.column_family.name] ||= []

        thrift_col = case mutation.action
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
          raise 'delete'
        else
          raise "dunno how to do #{mutation.inspect}"
        end

        y << thrift_col
      end

      mutation_map
    end
    
    def delete_row(klass, key)
      cf = get_column_family(klass)
      
      # TODO: "Deletion does not yet support SliceRange predicates."
      
      # deletion = CassandraThrift::Deletion.new(
      #   :predicate => CassandraThrift::SlicePredicate.new(
      #     :slice_range => CassandraThrift::SliceRange.new(
      #       :start => '',
      #       :finish => '',
      #       :count => 2147483647 # Integer.MAX_VALUE
      #     )
      #   ),
      #   :timestamp => timestamp
      # )
      # 
      # mutation = CassandraThrift::Mutation.new(
      #   :deletion => deletion
      # )
      # 
      # mutation_map = { key.to_s => { cf.name => [mutation] } }
      # 
      # @thrift.batch_mutate(@keyspace, mutation_map, CassandraThrift::ConsistencyLevel::ONE)
      
      # easiest way to enforce beeing inside a mutation, yet we are not actually mutating
      # FIXME: wtf ;)      
      current_mutation
      
      _delete_row(cf, key)
    end
    
    def _delete_row(cf, key)
      column_path = CassandraThrift::ColumnPath.new(
        :column_family => cf.name
      )
      
      @thrift.remove(@keyspace, key.to_s, column_path, timestamp, CassandraThrift::ConsistencyLevel::ONE)
    end
    
    def delete_all_rows!(klass)
      cf = get_column_family(klass)
      
      # FIXME: wtf ;)      
      current_mutation
      
      column_parent = CassandraThrift::ColumnParent.new(
        :column_family => cf.name
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
        # FIXME: this is really bad ;)
      )
      
      @thrift.get_range_slices(@keyspace, column_parent, predicate, key_range, CassandraThrift::ConsistencyLevel::ONE).each do |it|
        unless it.columns.empty?
          _delete_row(cf, it.key)
        end
      end
    end
    
    def get_column_family(klass)
      if klass.is_a?(ColumnFamily)
        cf = klass
      else
        cf = GreekArchitect::column_family(klass)
      end
      
      raise "#{klass} does not have a key value set! (use key :type)" if cf.key.nil?
      cf
    end
    
    def wrap(klass, key = nil)
      cf = get_column_family(klass)
      if klass.is_a?(ColumnFamily)
        klass = RowWrapper
      end
      
      klass.new(self, cf, key)
    end
    
    def wrap_custom(column_family, key = nil)
      RowWrapper.new(self, column_family, key)
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
  end
end