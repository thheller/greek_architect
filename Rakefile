PROJECT_ROOT = File.dirname(__FILE__)

task :thrift do
  puts "Generating Thrift bindings"
  system("rm -rf gen-rb &&
    thrift -gen rb #{PROJECT_ROOT}/server/interface/cassandra.thrift")
end