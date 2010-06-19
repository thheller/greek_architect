module GreekArchitect
  class List < RowWrapper
    
    def self.inherited(klass)
      klass.extend(ColumnFamilyClassMethods)
      klass.extend(SugarClassMethods)
      klass.extend(ListClassMethods)
    end

    # only works with compare_with types which can the auto created
    # uuid v1/v4, timestamp
    def append_value(value, timestamp = nil)
      columns.insert(column_family.compare_with.new_instance(), value, timestamp)
    end
  end
  
  module ListClassMethods
  end
end