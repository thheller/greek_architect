Gem::Specification.new do |s|
  s.name = "greek_architect"
  s.version = "0.0.1"
  s.date = "2010-06-15"
  s.authors = ["Thomas Heller"]
  s.email = "info@zilence.net"
  s.summary = "A simple Wrapper arround the Cassandra Thrift API."
  s.homepage = "http://github.com/thheller/greek_architect"
  s.description = "A simple Wrapper arround the Cassandra Thrift API."
  
  s.add_dependency('msgpack', '>= 0.4.2')
  s.add_dependency('thrift_client', '>= 0.4.3')
  
  s.files = [
    "README", "LICENSE", "Rakefile",
    "lib/greek_architect.rb",
    "lib/greek_architect/client.rb",
    "lib/greek_architect/column_family.rb",
    "lib/greek_architect/column_wrapper.rb",
    "lib/greek_architect/gen-rb/cassandra.rb",
    "lib/greek_architect/gen-rb/cassandra_constants.rb",
    "lib/greek_architect/gen-rb/cassandra_types.rb",
    "lib/greek_architect/hash.rb",
    "lib/greek_architect/list.rb",
    "lib/greek_architect/mutation.rb",
    "lib/greek_architect/row_wrapper.rb",
    "lib/greek_architect/types/abstract_type.rb",
    "lib/greek_architect/types/complex.rb",
    "lib/greek_architect/types/dates.rb",
    "lib/greek_architect/types/numbers.rb",
    "lib/greek_architect/types/strings.rb"
  ]
end