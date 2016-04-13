# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

require 'json'
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
    options, hosts = Cassandra::validate_and_massage_options(options)
  rescue => e
    futures = options.fetch(:futures_factory) { return Future::Error.new(e) }
    futures.error(e)
  else
    options[:cluster_klass] = Dse::Cluster
    driver = ::Cassandra::Driver.new(options)
    driver.connect(hosts)
  end
end

require 'dse/cluster'
require 'dse/session'
require 'dse/version'
require 'dse/graph/edge'
require 'dse/graph/path'
require 'dse/graph/result'
require 'dse/graph/result_set'
require 'dse/graph/vertex'
require 'dse/graph/vertex_property'