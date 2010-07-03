

class User < GreekArchitect::Row
  key :uuid
  
  column_family(:profile, :symbol) do |cf|
    cf.column :name, :string
    cf.column :created_at, :timestamp
  end
end
