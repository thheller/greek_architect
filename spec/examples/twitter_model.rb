
class User < GreekArchitect::Row
  key :uuid
  
  list(:following, :uuid, :string)
  list(:followers, :uuid, :string)
  
  list(:timeline, :time_uuid, :uuid)
  list(:tweets, :time_uuid, :uuid)
  list(:favorites, :time_uuid, :string)

  hash(:profile) do |cf|
    cf.column :name, :string
    cf.column :created_at, :timestamp
  end
  
  def most_recent_tweets()
    timeline.slice(:count => 5, :reversed => true).collect do |col|
      Tweet.get(col.value)
    end
  end
  
  def follow!(user)
    mutate do
      following.insert(user.key, 'hmmm')
      user.followers.insert(self.key, 'hmmm')
    end
  end
  
  def unfollow!(user)
    mutate do
      following.delete(user.key)
      user.followers.delete(self.key)
    end
  end
  
  def favorize!(tweet, reason = 'cause-i-can')
    mutate do
      favorites.insert(tweet.key, reason)
      
      tweet.favorized_by.append_value(self.key)
    end
  end
  
  def tweet!(message)
    tweet = Tweet.create()
    
    tweet.mutate() do
      tweet.message[:body] = message
      tweet.message[:created_at] = Time.now
      tweet.message[:created_by] = self.key
      
      timeline.append_value(tweet.key)
      tweets.append_value(tweet.key)
    end
    
    tweet.broadcast! # DJ: send_later(:broadcast!) RESQUE: .async(:broadcast!)
  end
end

class Tweet < GreekArchitect::Row
  key :time_uuid
  
  hash(:message) do |cf|
    cf.column :body, :string
    cf.column :created_at, :timestamp
    cf.column :created_by, :uuid
  end
  
  list(:favorized_by, :time_uuid, :uuid)
  
  # list :broadcasted_to, :compare_with => :time_uuid, :value_type => :reference
  
  def broadcast!
    mutate do
      User.get(message[:created_by]).followers.each do |col|
        User.get(col.name).timeline.append_value(self.key)
        
        # overkill! only to know who got it ... fun tho :P
        # at 1mil followers each tweet would be at least 50mb
        # (16byte col name + 16byte col value + 8byte col timestamp + x cassandra overhead) * 1mil
        # funny how much 140byte messages can grow ;)
        # broadcasted_to.append_value(follower)
      end
    end
  end
end