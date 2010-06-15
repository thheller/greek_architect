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
      @keyspace = keyspace
      @current_mutation = nil
    end
    
    def info(klass)
      schema[klass.to_s]
    end
    
    def schema()
      @schema ||= @thrift.describe_keyspace(@keyspace)
    end
    
    def get(column_family, key, column_path, consistency_level)    
      @thrift.get(@keyspace, column_family.key.encode(key), column_path, consistency_level)
    end
  
    def get_slice(column_family, key, column_parent, predicate, consistency_level)
      @thrift.get_slice(@keyspace, column_family.key.encode(key), column_parent, predicate, consistency_level)
    end
    
    def batch_mutate(mutation, consistency_level)
      @thrift.batch_mutate(@keyspace, mutation, consistency_level)
    end

    def wrap(klass, key = nil)
      # TODO: decide
      # I could keep an identity map instead of creating multiple instances
      # for the same row, not sure thats useful tho and would require cleanup
      cf = GreekArchitect::column_family(klass)

      raise "#{klass} does not have a key value set! (use key :type)" if cf.key.nil?
      
      klass.new(self, cf, key)
    end

    def mutate(consistency_level = CassandraThrift::ConsistencyLevel::ONE)
      raise AlreadyMutating, 'already mutating' unless @current_mutation.nil?

      @current_mutation = Mutation.new(self, consistency_level)
      begin
        yield()
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