# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

require 'bundler/setup'

require File.dirname(__FILE__) + '/../../support/retry.rb'
require File.dirname(__FILE__) + '/../../support/ccm.rb'

if RUBY_ENGINE == 'jruby'
  ENV['JRUBY_OPTS'] ||= '-Xcli.debug=true --debug'
end

unless ENV['COVERAGE'] == 'no' || RUBY_ENGINE == 'rbx'
  require 'simplecov'

  SimpleCov.start do
    command_name 'Cucumber'
  end
end

require 'aruba/cucumber'
require 'pathname'
require 'tempfile'
require 'yaml'

require 'cassandra'
require 'cassandra/compression/compressors/snappy'
require 'cassandra/compression/compressors/lz4'

if RUBY_ENGINE == 'rbx'
  class Aruba::ArubaPath
    def to_str
      to_s
    end
  end
end

AfterConfiguration do |configuration|
  slow_features = []

  features_files = configuration.feature_files.sort do |a, b|
    if slow_features.include?(a)
      1
    elsif slow_features.include?(b)
      -1
    else
      a <=> b
    end
  end

  # Get the singleton class/eigenclass for configuration
  klass = class << configuration; self; end

  klass.send(:undef_method, :feature_files)
  klass.send(:define_method, :feature_files) { features_files }
end

Aruba.configure do |config|
  config.exit_timeout = 60
end

Before do
  announcer.activate(:stdout)
  announcer.activate(:stderr)
end

After do |s|
  # Tell Cucumber to quit after this scenario is done - if it failed.
  Cucumber.wants_to_quit = true if s.failed? and ENV["FAIL_FAST"] == 'Y'
end

After('@auth') do
  $cluster.disable_authentication
end

Before do |scenario|
  if $dse_auth_enabled && !scenario.tags.map(&:name).include?('@dse_auth')
    $cluster.disable_dse_authentication
    $dse_auth_enabled = false
  end

  if $ldap_enabled && !scenario.tags.map(&:name).include?('@ldap_auth')
    $cluster.disable_ldap
    $ldap_enabled = false
  end

  if $kerberos_enabled && !scenario.tags.map(&:name).include?('@kerberos_auth')
    $cluster.disable_kerberos
    $kerberos_enabled = false
  end
end

After('@ssl') do
  $cluster.disable_ssl
end

After('@netblock') do
  $cluster.unblock_nodes
end

After('@client_failures') do
  $cluster.restart
end

