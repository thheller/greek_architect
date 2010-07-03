
class User < GreekArchitect::Row
  key :uuid
  
  column_family(:following, :uuid, :string)
  column_family(:followers, :uuid, :string)
  
  column_family(:timeline, :time_uuid, :uuid)
  column_family(:tweets, :time_uuid, :uuid)
  column_family(:favorites, :time_uuid, :string)

  column_family(:profile, :symbol) do |cf|
    cf.column :name, :string
    cf.column :created_at, :timestamp
  end
  
  def most_recent_tweets()
    list = timeline.slice(:count => 5, :reversed => true)
    list.collect do |col|
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
      
      # could be done in broadcast
      timeline.append_value(tweet.key)
      tweets.append_value(tweet.key)
    end
    
    tweet.broadcast! # DJ, RESQUE, etc: send_later(:broadcast!)
    tweet
  end
end

class Tweet < GreekArchitect::Row
  key :time_uuid
  
  column_family(:message, :symbol) do |cf|
    cf.column :body, :string
    cf.column :created_at, :timestamp
    cf.column :created_by, :uuid
  end
  
  column_family(:favorized_by, :time_uuid, :uuid)
  
  # column_family :broadcasted_to, :time_uuid, :uuid
  
  def broadcast!
    mutate do
      src = User.get(message[:created_by])
      
      # FIXME: implement each for UUIDv4
      src.followers.slice(:count => 100).each do |col|

        target = User.get(col.name)
        target.timeline.append_value(self.key)
        
        # overkill! only to know who got it ... fun tho :P
        # at 1mil followers each tweet would be at least 50mb
        # (16byte col name + 16byte col value + 8byte col timestamp + x cassandra overhead) * 1mil
        # funny how much 140byte messages can grow ;)
        # broadcasted_to.append_value(follower)
      end
    end
  end
end