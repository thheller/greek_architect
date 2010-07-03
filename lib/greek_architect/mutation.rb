
module GreekArchitect

  class ColumnMutation
    def initialize(action, column)
      @action = action
      @column = column
    end
    
    def row
      @column.row
    end
    
    def insert?
      action == :insert
    end
    
    def delete?
      action == :delete
    end
    
    attr_reader :action, :column
  end

  class Mutation
    def initialize(client, consistency_level)
      @client = client
      @mutations = []
      @consistency_level = consistency_level
    end
            
    attr_reader :consistency_level
    
    def append_insert(column)
      @mutations << ColumnMutation.new(:insert, column)
    end
    
    def append_delete(column)
      @mutations << ColumnMutation.new(:delete, column)
    end

    def execute!
      if @mutations.empty?
        return
      end
      
      # this is a VERY naive implementation
      # it WILL result in an infinite loop when try to mutate the column you were watching!
      
      # FIXME: move to a stack/phase model
      @mutations.each do |mutation|
        column_name = mutation.column.name
        column_family = mutation.column.column_family
        column_family.each_observer(column_name) do |obs|
          obs.call(mutation)
        end        
      end
      
      @client.batch_mutate(@mutations, consistency_level)      
    end
  end
end