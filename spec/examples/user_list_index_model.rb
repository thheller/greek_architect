class UserListIndex < GreekArchitect::List
  key :string

  compare_with :time_uuid
  value_type :uuid
  
  class << self
    def each_user_created_at(time)
      partition = get_partition_key(time)
      
      UserListIndex.get(partition).slice(:count => 100).each do |it|
        yield(User.get(it.value.to_s))
      end
    end
    
    def get_partition_key(time)
      partition = time.strftime('%Y%m%d')
    end
  end
  
  on_mutation_of(User, :created_at) do |mutation|
    partition = get_partition_key(mutation.column.value)
    UserListIndex.get(partition).append_value(mutation.row.key)
  end
end


  
