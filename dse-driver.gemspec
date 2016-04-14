# encoding: utf-8

$LOAD_PATH << File.expand_path('../lib', __FILE__)

require 'dse/version'

Gem::Specification.new do |s|
  s.name          = 'dse-driver'
  s.version       = Dse::VERSION.dup
  s.authors       = ['Sandeep Tamhankar']
  s.email         = ['sandeep.tamhankar@datastax.com']
  #  s.homepage      = 'http://datastax.github.io/ruby-driver'
  s.summary       = 'Ruby Driver for DataStax Enterprise'
  s.description   = 'A pure Ruby driver for DataStax Enterprise'
  #  s.license       = 'Apache License 2.0'
  s.files         = Dir['lib/**/*.rb', 'README.md', '.yardopts']
  s.require_paths = %w(lib)

  s.extra_rdoc_files = ['README.md']
  s.rdoc_options << '--title' << 'Ruby Driver for DSE' << '--main' << 'README.md' << '--line-numbers'

  s.required_ruby_version = '>= 1.9.3'

  s.add_runtime_dependency 'cassandra-driver', '~> 3.0.1'

  s.add_development_dependency 'bundler', '~> 1.6'
  s.add_development_dependency 'rake', '~> 10.0'
end
