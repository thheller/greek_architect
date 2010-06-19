
class UserNameIndex < GreekArchitect::Hash
  key :string

  compare_with :symbol
  column :user_id, :uuid

  class << self
    def get_user(name)
      idx = get(name)
      
      user = User.get(idx.user_id)
      if user.exists?
        return user
      end

      nil
    end
  end

  on_mutation_of(User, :name) do |mutation|
    new_user_name = mutation.column.value

    if mutation.insert?
      if prev_name = mutation.column.previous_value
        UserNameIndex.delete_row(prev_name)
      end

      idx = UserNameIndex.get(new_user_name)
      idx[:user_id] = mutation.row.key

    elsif mutation.delete?      
      UserNameIndex.delete_row(mutation.column.value)
    end
  end
end
