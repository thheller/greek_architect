
describe_cluster "Test Cluster" do

  server '127.0.0.1:9160'
  
  
  keyspace 'UserDemo' do
    replication_factor 1

    stores [User]
  end
end