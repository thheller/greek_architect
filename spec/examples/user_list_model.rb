
# '2010-07-03' => users => [{'time_uuid_1', 'uuid_of_user1', ts}, {'time_uuid_2', 'uuid_of_user2', ts}]

class ByCreationDate < GreekArchitect::Row
  key :string

  column_family(:users, :time_uuid, :uuid)

  class << self
    def walk_users_created_at(time)
      partition = get_partition_key(time)
      
      get(partition).users.each do |it|
        yield(User.get(it.value))
      end
    end
    
    def get_partition_key(time)
      partition = time.strftime('%Y%m%d')
    end
  end
  
  on_mutation_of(User, :profile, :created_at) do |mutation|
    created_at = mutation.column.value
    
    partition = get_partition_key(created_at)
    uuid = mutation.row.key
    
    ByCreationDate.get(partition).users.append_value(uuid)
  end
end


  
