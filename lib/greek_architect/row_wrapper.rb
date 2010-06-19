module GreekArchitect
  
  class Columns
    
    def initialize(client, row)
      @columns = {}
      @client = client
      @row = row
    end
    
    def insert(column_name, column_value, timestamp = nil)
      col = column_wrapper(column_name)

      if column_value.nil?
        col.delete!
      else
        col.insert(column_value, timestamp || @client.timestamp)
      end      
    end
    
    def [](key)
      get(key, nil)
    end
    
    def get_value(column_name, consistency_level = nil)
      if x = get(column_name, consistency_level)
        return x.value
      end
    end
    
    def get(column_name, consistency_level = nil)
      if tcol = @client.get(@row, column_name, consistency_level)
        col = column_wrapper(column_name)
        col.load_raw_values(tcol.column.name, tcol.column.value, tcol.column.timestamp)
        return col
      end
      
      nil
    end

    def column_wrapper(column_name)
      @columns[column_name] ||= begin
        if not value_type = @row.column_family.named_columns[column_name]
          if not value_type = @row.column_family.value_type
            raise "#{self.class} does not define a #{column_name} column and no default value_type is set"
          end
        end
        
        col = ColumnWrapper.new(@client, @row)
        col.init_with_name(column_name)
        col
      end
    end
    
    
    #
    #def load_columns()
    #  if not @loaded
    #    # columns_to_load = column_family['columns']
    #    
    #    predicate = CassandraThrift::SlicePredicate.new(
    #      # leave the optimization stuff for later, for now just load EVERY column!
    #      # :column_names => columns_to_load.collect { |it| column_family.compare_with.encode(it) }
    #      
    #      :slice_range => CassandraThrift::SliceRange.new(
    #        :start => '',
    #        :finish => '',
    #        :count => 1000 # FIXME: what if we have more than that? its a hash so you souldnt, but you may
    #      )
    #    )
    #  
    #    client.get_slice(column_family, key, column_parent, predicate, read_consistency_level).collect do |it|
    #      
    #      name = column_family.compare_with.decode(it.column.name)
    #    
    #      if not value_type = column_family["column:#{name}"]
    #        if not value_type = column_family["value_type"]
    #          raise "#{self.class} does not define a #{name} column and no default value_type is set"
    #        end
    #      end
    #
    #      @columns[name] = col = ColumnWrapper.new(self, column_family.compare_with, value_type)
    #      col.load(it.column)
    #      
    #      columns_loaded << name
    #    end
    #    
    #    @loaded = true
    #    
    #    # columns_loaded.concat(columns_to_load)
    #  end
    #  
    #  @columns
    #end    
  end
  
  class RowWrapper
    def initialize(client, column_family, key)
      @client = client
      @column_family = column_family
      @key = key.nil? ? column_family.key.new_instance() : key
      @columns = Columns.new(@client, self)
    end

    attr_reader :column_family, :key, :columns

    def id
      key
    end
    
    # you might want better consistency when checking if an object exists
    def exists?(consistency_level = nil)
      # suggested by stuhood on #cassandra
      # if theres any column in this row we declare it as existant
      # faster than doing a column_count() > 0
      slice(:count => 1).any?
    end
    
    alias_method :present?, :exists?
    
    def column_count(consistency_level = nil)
      @client.get_count(column_family, key, consistency_level)
    end
    
    def mutate(write_consistency_level = nil)
      @client.mutate(write_consistency_level) do
        yield()
      end
    end

    def slice(opts = {})      
      @client.get_slice(self, opts)
    end

    def insert(name, value, timestamp = nil)
      columns.insert(name, value, timestamp)
    end
  end
  
  # this provides some syntactic sugar
  # User.get(uid) || User.create()
  # instead of
  # client.wrap(User, uid) || client.wrap(User)
  
  # makes it look more like ActiveRecord & Co.
  # but I definititly want everything to be usable without this!
  
  module SugarClassMethods
    def create()
      GreekArchitect::wrap(self, nil)
    end
    
    def get(key)
      GreekArchitect::wrap(self, key)
    end
    
    def get_column(key, column_name)
      get(key).columns[column_name]
    end
    
    def delete_row(key)
      GreekArchitect::get_active_client(self).delete_row(self, key)
    end
    
    def delete_all_rows!
      GreekArchitect::get_active_client(self).delete_all_rows!(self)
    end
  end
  
  module Sugar
    def self.included(klass)
      klass.extend(SugarClassMethods)
    end
  end
  
  module ColumnFamilyClassMethods

    def value_type(typename, opts = {})
      cf = GreekArchitect::column_family(self)
      gtype =  GreekArchitect::type_by_name(typename)
      cf.value_type = gtype.configure(opts)
    end    
    
    def override_name(new_name)
      cf = GreekArchitect::column_family(self)
      cf.name = new_name
    end
    
    def key(typename, opts = {})
      cf = GreekArchitect::column_family(self)
      gtype =  GreekArchitect::type_by_name(typename)
      cf.key = gtype.configure(opts)
    end
    
    def compare_with(typename, opts = {})
      cf = GreekArchitect::column_family(self)
      gtype =  GreekArchitect::type_by_name(typename)
      cf.compare_with = gtype.configure(opts)
    end  
    
    def on_mutation_of(cf_name, column, &block)
      cf = GreekArchitect::column_family(cf_name)
      cf.register_observer(column, block)
    end
  end
end
