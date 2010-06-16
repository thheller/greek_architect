
"""
A GreekArchitect::Hash desribes a data structure which closely resembles a Hash! orly :P

Behaviour is this (for now):

Lazy loading ALL columns for a row when ANY column is accessed

This implies that you only want to use this if its ok to load
the entire row into memory and transport it from the server to the
client whenever you access it.

"""
# to make this work you need a ColumnFamily named User in Cassandra

# CompareWith=AsciiType/BytesType/UTF8Type (Ascii should do fine)

module GreekArchitectByExample
  
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

    # alternatives, I choose msgpack because its is more compact and faster
    # value_type :json
    # value_type :string
  end
end