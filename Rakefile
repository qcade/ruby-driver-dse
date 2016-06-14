# encoding: utf-8

require 'bundler/setup'

require 'rspec/core/rake_task'
require 'cucumber/rake/task'
require 'rake/testtask'
require 'bundler/gem_tasks'

ENV['FAIL_FAST'] ||= 'Y'

desc 'Run all tests'
task test: [:rspec, :integration, :cucumber]

ruby_engine = defined?(RUBY_ENGINE) ? RUBY_ENGINE : 'ruby'

case ruby_engine
when 'jruby'
  require 'rake/javaextensiontask'
  #
  # Rake::JavaExtensionTask.new('gss_api_context')

  Rake::TestTask.new(:integration) do |t|
    t.libs.push 'lib'
    t.test_files = FileList['integration/*_test.rb',
                            'integration/security/*_test.rb']
    t.verbose = true
  end
  RSpec::Core::RakeTask.new(:rspec)
  Cucumber::Rake::Task.new(:cucumber)
else
  require 'rake/extensiontask'

  Rake::ExtensionTask.new('gss_api_context')

  Rake::TestTask.new(:integration => :compile) do |t|
    t.libs.push 'lib'
    t.test_files = FileList['integration/*_test.rb',
                            'integration/security/*_test.rb']
    t.verbose = true
  end
  RSpec::Core::RakeTask.new(:rspec => :compile)
  Cucumber::Rake::Task.new(:cucumber => :compile)
end

