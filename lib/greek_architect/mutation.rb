
module GreekArchitect
  # represents a batch mutation
  class MutationEntry
    def initialize(column_family, key, mutation)
      @column_family = column_family
      @key = key
      @mutation = mutation
    end
    
    attr_reader :column_family, :key, :mutation
    
    def key_raw()
      column_family.key.encode(@key)
    end
    
    def inspect
      %{\#<MutationEntry:#{object_id}
        @column_family=#{@column_family.name.inspect}
        @key=#{@key.inspect}
        @mutation=#{@mutation.inspect}>}
    end
  end

  class Mutation
    def initialize(client, consistency_level)
      @client = client
      @mutations = []
      @consistency_level = consistency_level
      # @max_attempts = 1
    end
        
    attr_reader :consistency_level
    
    def append(cf, key, mutation)
      raise "need column family to append, got #{cf.class}" unless cf.is_a?(ColumnFamily)
      
      @mutations << MutationEntry.new(cf, key, mutation)
    end
    
    def generate_mutation_map()
      mutation_map = {}
      
      @mutations.each do |it|
        x = mutation_map[it.key_raw] ||= {}
        y = x[it.column_family.name] ||= []
        y << it.mutation
      end
      
      mutation_map
    end

    def execute!
      # TODO: automatic retry should something fail
      # TODO: if it fails permanently we could log the mutations for debug purposes
      # but honestly everything that goes into a mutation SHOULD be checked anyways
      # so the only failures would be node down and such
      # which should be fine after retrying another node
      
      mutation_map = generate_mutation_map()
      
      # attempts = 0
      # pp @mutations
      
      puts "mutating #{@mutations.length} entries"
      
      # begin
      @client.batch_mutate(mutation_map, consistency_level)      
      # rescue
        # raise if attempts > @max_attempts
        # inc attempts
        # retry
      # end
    end
  end
end