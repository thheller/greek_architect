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