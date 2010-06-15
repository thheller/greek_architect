
module Greek
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
      cf = Greek::column_family(klass)
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