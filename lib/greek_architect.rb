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

  class Type
    def initialize(typename, java_class, ruby_type)
      @typename = typename
      @java_class = java_class
      @ruby_type = ruby_type
    end
        
    attr_reader :typename, :java_class, :ruby_type
    
    def configure(opts)
      TypeInstance.new(self, ruby_type.new(opts))
    end
  end
  
  class TypeInstance
    def initialize(greek_type, wrapper)
      @greek_type = greek_type
      @wrapper = wrapper 
    end
    
    attr_reader :greek_type, :wrapper
    
    def new_instance()
      wrapper.new_instance()
    end
    
    def encode(value)
      raise "#{self.class} cannot encode nil values" if value.nil?
      
      wrapper.encode(value)
    end
    
    def decode(value)
      raise "#{self.class} cannot decode nil values" if value.nil?
      
      wrapper.decode(value)
    end
  end

  
  class << self
    def connect(keyspace, server)
      if keyspace.is_a?(Module)
        active_clients[keyspace.to_s] = Client.connect(server, keyspace.to_s)
      else
        active_clients['default'] = Client.connect(server, keyspace)
      end
    end
    
    def get_active_client(row_class)
      # FIXME: is there no simpler way to to this?
      if row_class.to_s =~ /(.+)::(.+)/
        module_name = $1
      else
        module_name = 'default'
      end
      
      if client = active_clients[module_name]
        return client
      else
        raise "#{module_name}::#{row_class} does not have an active connection!"
      end
    end

    def wrap(row_class, key)
      get_active_client(row_class).wrap(row_class, key)
    end
    
    def active_clients
      @@active_clients ||= {}
    end

    def type_by_name(typename)
      if not x = greek_types.detect { |it| it.typename == typename }
        raise ArgumentError, "#{self}.key no such greek type: #{typename}", caller
      end
      x
    end

    
    def column_family(type)
      (@@column_family ||= {})[type] ||= begin
        column_family_name = if type.to_s =~ /(.+)::(.+)/
          $2
        else
          type.to_s
        end        
        ColumnFamily.new(column_family_name)
      end
    end
    
    def register_type(typename, ruby_type, java_class)
      (@@greek_types ||= []) << Type.new(typename, java_class, ruby_type)
    end
    
    def greek_types()
      (@@greek_types ||= [])
    end
  end
end


require 'greek_architect/client'
require 'greek_architect/column_family'
require 'greek_architect/row_wrapper'
require 'greek_architect/column_wrapper'
require 'greek_architect/mutation'
require 'greek_architect/types/abstract_type'
require 'greek_architect/types/numbers'
require 'greek_architect/types/strings'
require 'greek_architect/types/dates'
require 'greek_architect/types/complex'
require 'greek_architect/list'
require 'greek_architect/hash'


