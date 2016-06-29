# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

require 'json'
require 'gss_api_context' unless RUBY_ENGINE == 'jruby'
require 'cassandra'

module Dse
  # Creates a {Dse::Cluster Cluster instance}, which extends Cassandra::Cluster
  # <http://datastax.github.io/ruby-driver/api/cassandra/cluster>`.
  # The API is identical, except that it returns a `Dse::Session` (see below). It takes all of the same options
  # as Cassandra.cluster and the following extra options.
  #
  # @option options [Dse::Graph::Options] :graph_options options for the DSE graph statement handler. Takes
  #    priority over other `:graph_*` options specified below.
  # @option options [String] :graph_name name of graph to use in graph statements
  # @option options [String] :graph_source graph traversal source
  # @option options [String] :graph_language language used in graph queries
  # @option options [Cassandra::CONSISTENCIES] :graph_read_consistency read consistency level for graph statements.
  #    Overrides the standard statement consistency level
  # @option options [Cassandra::CONSISTENCIES] :graph_write_consistency write consistency level for graph statements.
  #    Overrides the standard statement consistency level
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
    graph_options = if !options[:graph_options].nil?
                      Cassandra::Util.assert_instance_of(Dse::Graph::Options, options[:graph_options])
                      options[:graph_options]
                    else
                      Dse::Graph::Options.new(options)
                    end
    username = options[:username]
    password = options[:password]
    options[:custom_types] ||= []
    options[:custom_types] << Dse::Geometry::Point << Dse::Geometry::LineString << Dse::Geometry::Polygon
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
    future = driver.connect(hosts)
    future.then do |cluster|
      cluster.graph_options.merge!(graph_options)
      cluster
    end
  end
end

require 'dse/cluster'
require 'dse/util/endian_buffer'
require 'dse/geometry/line_string'
require 'dse/geometry/point'
require 'dse/geometry/polygon'
require 'dse/session'
require 'dse/version'
require 'dse/graph'
require 'dse/load_balancing/policies/host_targeting'
require 'dse/statements'
require 'dse/auth/providers/gss_api'
require 'dse/auth/providers/password'
