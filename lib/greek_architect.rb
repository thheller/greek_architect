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

$LOAD_PATH << File.expand_path(File.dirname(__FILE__))

require 'rubygems'
require 'thrift_client'
require 'uuid'
require 'greek_architect/gen-rb/cassandra_constants'
require 'greek_architect/gen-rb/cassandra_types'
require 'greek_architect/gen-rb/cassandra'

module GreekArchitect
  class AlreadyMutating < StandardError
  end
  
  class NotMutating < StandardError
  end

  class << self
    def runtime()
      Runtime.instance
    end
    
    def connect(keyspace, server)
      runtime.connect(keyspace, server)
    end
  end
end

require 'greek_architect/runtime'
require 'greek_architect/client'
require 'greek_architect/column_family'
require 'greek_architect/row'
require 'greek_architect/column_wrapper'
require 'greek_architect/mutation'
require 'greek_architect/types/abstract_type'
require 'greek_architect/types/numbers'
require 'greek_architect/types/strings'
require 'greek_architect/types/dates'
require 'greek_architect/types/complex'
#require 'greek_architect/list'
#require 'greek_architect/hash'


