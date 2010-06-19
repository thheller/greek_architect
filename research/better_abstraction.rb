
class User < GreekArchitect::Row
  key :uuid

  unique_index :by_login, '/account/login'
  
  member :account
  member :profile
end


class Account < GreekArchitect::ColumnFamily
  acts_as :hash # implies CompareWith=AsciiType since column names are symbols

  column :login, :string
  column :password, :string
  column :created_at, :timestamp
end


class Profile < GreekArchitect::ColumnFamily
  acts_as :hash

  column :name, :string
  column :date_of_birth, :date
  column :image, :row_reference
end

class Image < GreekArchitect::Row
  key :uuid
  
  list :by_owner, '/metadata/owner', :sort_by => :created_at, :reversed => true
  
  member :metadata
end

class Metadata < GreekArchitect::ColumnFamily
  acts_as :hash

  column :owner, :row_reference
  column :category, :string
  column :dimensions, :string
  column :created_at, :timestamp
end


user = User.new()
user.mutate do
  account = user.account # no create necessary, we know the row already
  account.login = 'info@zilence.net'
  account.password = 'supersecret'
  account.created_at = Time.now

  profile = user.profile
  profile.name = 'Thomas Heller'
  profile.date_of_birth = Date.parse('1979/01/19')
end


image = Image.new()
image.mutate do
  image.metadata.owner = user
  image.metadata.dimensions = '320x240'
  image.metadata.byte_size = 123123
  image.metadata.created_at = Time.now

  File.open('uploaded_via_http', 'r') do |file|
    image.file.data = file.read
    image.file.name = file.name
  end
  
  user.profile.image = image
end


user = User.by_login('info@zilence.net')
newest_images = Image.by_owner(user).slice(:count => 5)