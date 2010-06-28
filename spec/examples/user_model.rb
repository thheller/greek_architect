

class User < GreekArchitect::Row
  key :uuid
  
  hash(:profile) do |cf|
    cf.column :name, :string
    cf.column :created_at, :timestamp
  end
end
