
# TODO: this is not pretty enough yet!

__END__

it "example: how to access users by name" do
  # BEWARE: HARDCORE DETAILS
  # THIS COULD BE AUTOMATED AT SOME POINT, BUT I REALLY DO NOT LIKE MAGIC!

  # Step1: create our name index
  class UserNameIndex < GreekArchitect::Hash # will create an GreekArchitect::UniqueIndex in the future
    key :string
    compare_with :symbol
    column :user_id, :uuid
    
    def user
      client.wrap(User, user_id)
    end
  end
  
  # Step2: write the index when writing the name
  class User
    alias_method :really_set_name, :name=

    def name=(value)
      idx = client.wrap(UserNameIndex, value)
      really_set_name(value)
      idx[:user_id] = self.key
    end
  end
  
  user = User.wrap()
  GreekArchitect.mutate do
  end
  
  # thats that
  zilence = @client.wrap(User)
  
  @client.mutate do
    zilence.name = 'zilence'
    zilence.created_at = Time.now
  end
  
  by_id = @client.wrap(User, zilence.id)
  by_id.name.should == 'zilence'
  
  # this really could use some sugar    
  by_name = @client.wrap(UserNameIndex, 'zilence')
  by_name.user.name.should == 'zilence'
  
  # IMPORTANT: this is not how you build and index properly
  # you NEED to remove the old name entry, otherwise the old one will still work
  # consider this a TODO!
end