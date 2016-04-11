source 'https://rubygems.org/'

gemspec

gem 'snappy',        :group => [:development, :test]
gem 'lz4-ruby',      :group => [:development, :test]
gem 'rake-compiler', :group => [:development, :test]
gem 'cliver',        :group => [:development, :test]

# This is temporary until the driver is available on RubyGems.
#gem 'cassandra-driver', '3.0.1', :git => '/Users/sandeeptamhankar/ruby-driver', :branch => '3.0.1'
gem 'cassandra-driver', '3.0.1', :git => 'http://github.com/datastax/ruby-driver.git', :branch => '3.0.1'

group :development do
  platforms :mri_19 do
    gem 'perftools.rb'
  end
  gem 'rubocop', '~> 0.36', require: false
end

group :test do
  gem 'rspec'
  gem 'rspec-wait'
  gem 'rspec-collection_matchers'
  gem 'simplecov'
  gem 'cucumber'
  gem 'aruba'
  gem 'os'
  gem 'minitest', '< 5.0.0'
  gem 'ansi'
end

group :docs do
  gem 'yard'
end
