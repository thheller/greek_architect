module GreekArchitect
  module ActsAsRow    
    def self.included(klass)
      row_config = GreekArchitect::runtime.get_row_config(klass)
      row_config.key_type = GreekArchitect::Types::Integer.new()
      
      klass.extend(RowHelperClassMethods)
    end
    
    def greek_architect_row_config()
      @greek_architect_row_config ||= GreekArchitect::runtime.get_row_config(self.class)
    end
    
    def greek_architect_row()
      @greek_architect_row ||= begin
        GreekArchitect::runtime.client().wrap_row(greek_architect_row_config, self.id)
      end
    end
    
    def key
      id
    end
    
    def mutate(write_consistency_level = :one)
      greek_architect_row.mutate(write_consistency_level) do
        yield()
      end
    end
  end
end