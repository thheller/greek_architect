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
  class NotConnected < StandardError; end
  
  class TypeInfo
    def initialize(typename, java_class, ruby_type)
      @typename = typename
      @java_class = java_class
      @ruby_type = ruby_type
    end
        
    attr_reader :typename, :java_class, :ruby_type
    
    def configure(opts = {})
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
    
    def incr(value)
      if wrapper.respond_to?(:incr)
        wrapper.incr(value)
      else
        raise ArgumentError, "#{@greek_type.ruby_type} does not support incrementing, each not supported, use slice"
      end
    end
    
    def convert(value)
      if value.nil?
        wrapper.new_instance()
      else
        wrapper.convert(value)
      end
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
  
  class ConfigBuilder
    def initialize()
      @scope = []
    end
    
    def describe_cluster(name)
      @scope << ClusterBuilder.new(name)
      
      yield()
    end
    
    def method_missing(m, *args)
      current_target = @scope.last
      if current_target.respond_to?(m)
        @scope.last.send(m, *args)
      else
        raise "#{current_target.class} does not respond to #{m}"
      end
    end
  end
  
  class ClusterBuilder
    def initialize(name)
      @name = name
      @keyspaces = []
    end
    
    def server(addr)
      @servers << addr
    end
  end
  
  class Runtime
    class << self
      def instance()
        @@runtime ||= Runtime.new()
      end
      
      def method_missing(m, *args)
        instance.send(m, *args)
      end
      
      protected :new
    end
    
    def initialize()
      @registered_types = {}
      @row_configs = {}
      
      @client = nil
      @servers = nil
      @keyspace = nil
    end
    
    def configure(config)
      
      cb = ConfigBuilder.new()
      cb.instance_eval(File.read(config))
      p cb
      raise 'boom'
      disconnect!
      
      if options.nil? or options.empty?
        raise "invalid cassandra config, need at least keyspace/servers"
      end
      
      servers = options['servers']
      keyspace = options['keyspace']
      
      if !servers.is_a?(Array) or servers.empty?
        raise "cassandra:servers cannot be empty and must be an array: #{servers.inspect}"
      end
      
      if !keyspace.is_a?(String)
        raise "cassandra:keyspace must be a string: #{keyspace.inspect}"
      end
      
      @servers = servers
      @keyspace = keyspace
    end
    
    def disconnect!
      @client.disconnect! if @client
      @client = nil
    end

    def client
      @client ||= Client.new(@keyspace, @servers)
    end
    
    def get_row_config(klass)
      raise 'not a class' unless klass.is_a?(Class)
      
      @row_configs[klass.to_s] ||= RowConfig.new(klass.to_s)
    end
    
    def get_type_by_name(typename)
      if not x = @registered_types[typename]
        raise ArgumentError, "no such greek type: #{typename}", caller
      end
      
      x
    end    
    
    def register_type(typename, ruby_type, java_class)
      @registered_types[typename] = TypeInfo.new(typename, java_class, ruby_type)
    end
  end
end