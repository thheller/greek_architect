require File.dirname(__FILE__) + "/lib/greek_architect.rb"

cassandra = JSON.parse(File.read("#{Rails.root}/config/cassandra.json"))
GreekArchitect::runtime.configure(cassandra[Rails.env])

puts "Greek'd"