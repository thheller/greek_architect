
class ByName < GreekArchitect::Row
  key :string

  column_family(:users, :string, :uuid)

  class << self
    
    def get_partition(name)
      # for demo use one row
      # partition by first(1-3) letters or something fancy to avoid having all users in one row
      
      get("one-ring-to-rule-them-all")
    end
    
    def get_user(name)
      idx = get_partition(name)
      
      if uuid = idx.users[name]
        return User.get(uuid)
      end
      
      nil
    end
  end

  on_mutation_of(User, :profile, :name) do |mutation|
    new_user_name = mutation.column.value

    if mutation.insert?
      if prev_name = mutation.column.previous_value
        get_partition(prev_name).users.delete(prev_name)
      end

      idx = get_partition(prev_name)
      idx.users.insert(new_user_name, mutation.row.key)

    elsif mutation.delete?      
      get_partition(new_user_name).users.delete(new_user_name)
    end
  end
end
