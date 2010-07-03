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