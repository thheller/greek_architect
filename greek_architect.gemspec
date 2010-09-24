# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name        = 'greek_architect'
  s.version     = '0.1.0'
  s.authors     = ["Thomas Heller"]
  s.description = 'experimental Cassandra Client'
  s.summary     = "http://github.com/thheller/greek_architect"
  s.email       = 'info@zilence.net'
  s.homepage    = s.summary

  s.platform    = Gem::Platform::RUBY

  s.add_dependency 'thrift', '~> 0.2.0.4'
  s.add_dependency 'json', '~> 1.4.3'
  s.add_dependency 'msgpack', '~> 0.4.3'
  
  s.add_development_dependency 'rake', '~> 0.8.7'
  s.add_development_dependency 'rspec', '~> 2.0.0.beta.20'
  s.add_development_dependency 'spork', '~> 0.8.4'
  s.add_development_dependency 'rcov', '~> 0.9.9'

  s.rubygems_version   = "1.3.7"
  s.files            = `git ls-files`.split("\n")
  s.test_files       = `git ls-files -- {specs}/*`.split("\n")
  s.executables      = []
  s.extra_rdoc_files = ["LICENSE", "README"]
  s.rdoc_options     = ["--charset=UTF-8"]
  s.require_path     = "lib"
end

