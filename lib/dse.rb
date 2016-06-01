# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

require 'json'
require 'gss_api_context'
require 'cassandra'

module Dse
  # Creates a {Dse::Cluster Cluster instance}, which extends Cassandra::Cluster
  # <http://datastax.github.io/ruby-driver/api/cassandra/cluster>`.
  # The API is identical, except that it returns a `Dse::Session` (see below).
  #
  # @example Connecting to localhost
  #   cluster = Dse.cluster
  #
  # @example Configuring {Dse::Cluster}
  #   cluster = Dse.cluster(
  #               username: username,
  #               password: password,
  #               hosts: ['10.0.1.1', '10.0.1.2', '10.0.1.3']
  #             )
  #
  # @return [Dse::Cluster] a cluster instance
  def self.cluster(options = {})
    cluster_async(options).get
  end

  # Creates a {Dse::Cluster Cluster instance}.
  #
  # @see Dse.cluster
  #
  # @return [Cassandra::Future<Dse::Cluster>] a future resolving to the
  #   cluster instance.
  def self.cluster_async(options = {})
    username = options[:username]
    password = options[:password]
    options, hosts = Cassandra.validate_and_massage_options(options)

    # Use the DSE plain text authenticator if we have a username and password. The above validation already
    # raises an error if one is given without the other.
    options[:auth_provider] = Auth::Providers::Password.new(username, password) if username && password
  rescue => e
    futures = options.fetch(:futures_factory) { return Cassandra::Future::Error.new(e) }
    futures.error(e)
  else
    options[:cluster_klass] = Dse::Cluster
    driver = ::Cassandra::Driver.new(options)

    # Wrap the load-balancing policy that we'd otherwise run with, with a host-targeting policy.
    # We do this before driver.connect because driver.connect saves off the policy in the cluster
    # registry and does a few other things.

    lbp = driver.load_balancing_policy
    driver.load_balancing_policy = Dse::LoadBalancing::Policies::HostTargeting.new(lbp)
    driver.connect(hosts)
  end
end

require 'dse/cluster'
require 'dse/session'
require 'dse/version'
require 'dse/graph'
require 'dse/load_balancing/policies/host_targeting'
require 'dse/statements'
require 'dse/auth/providers/gss_api'
require 'dse/auth/providers/password'
