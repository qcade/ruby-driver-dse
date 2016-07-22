# encoding: utf-8

require 'bundler/setup'

require 'rspec/core/rake_task'
require 'cucumber/rake/task'
require 'rake/testtask'
require 'bundler/gem_tasks'

ENV['FAIL_FAST'] ||= 'Y'

RSpec::Core::RakeTask.new(:rspec => :compile)

Cucumber::Rake::Task.new(:cucumber => :compile)

desc 'Run all tests'
task test: [:rspec, :integration, :cucumber]

ruby_engine = defined?(RUBY_ENGINE) ? RUBY_ENGINE : 'ruby'

case ruby_engine
when 'jruby'
  require 'rake/javaextensiontask'

  Rake::JavaExtensionTask.new('challenge_evaluator')
else
  require 'rake/extensiontask'

  Rake::ExtensionTask.new('gss_api_context')
end

Rake::TestTask.new(:integration => :compile) do |t|
  t.libs.push 'lib'
  t.test_files = FileList['integration/*_test.rb',
                          'integration/authentication/*_test.rb']
  t.verbose = true
end

