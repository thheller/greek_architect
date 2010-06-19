
# to make this work you need a ColumnFamily named User in Cassandra

# CompareWith=AsciiType/BytesType/UTF8Type (Ascii should do fine)

class User < GreekArchitect::Hash
  # the unique id for the user should be a uuid (globally unique)
  # Cassandra: this is the row key!
  key :uuid

  # use ruby symbols to address values in our hash
  # Cassandra: this is CompareWith translated to a Ruby class.
  compare_with :symbol
  # :sybmbol since we want to address the values in our hash by symbol

  # we can name columns which allows for some sugar  
  column :created_at, :timestamp
  column :name, :string

  # and we can define a general value type
  # using msgpack here since this keeps the basic Ruby Type alive
  # aka when inserting Int/Time/String we get Int/Time/String back!
  value_type :msgpack

  # BEWARE THIS IS DANGEROUS!!!
  # its also totally awesome ;)
  # TODO: explain
  
  # alternatives, I choose msgpack because its is more compact and faster
  # value_type :json
  # value_type :string
end
