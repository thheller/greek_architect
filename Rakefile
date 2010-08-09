PROJECT_ROOT = File.dirname(__FILE__)

task :thrift do
  puts "Generating Thrift bindings"
  system("rm -rf #{PROJECT_ROOT}/lib/cassandra-gen-rb/0.7/* && mkdir -p #{PROJECT_ROOT}/lib/cassandra-gen-rb/0.7 &&
    thrift -gen rb -o #{PROJECT_ROOT}/lib/cassandra-gen-rb/0.7 #{PROJECT_ROOT}/etc/cassandra.thrift")
end

task :find_all_files do
  p Dir['lib/**/**.rb'].sort
end

task :rcov do
  system("rcov -x gems -x spec #{Dir['spec/**/**spec.rb'].sort.join(" ")}")
end

task :default => [:test] do
end

task :test do
  system("rspec spec examples/twitter")
end