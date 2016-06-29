# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

module Dse
  # Cluster represents a DSE cluster. It serves as a
  # {Dse::Session session factory} and a collection of metadata. It wraps
  # a {http://datastax.github.io/ruby-driver/api/cassandra/cluster Cassandra::Cluster} and exposes all of its
  # functionality.
  class Cluster
    # @return [Dse::Graph::Options] default graph options used by queries on this cluster.
    attr_reader :graph_options

    # @private
    def initialize(logger,
                   io_reactor,
                   executor,
                   control_connection,
                   cluster_registry,
                   cluster_schema,
                   cluster_metadata,
                   execution_options,
                   connection_options,
                   load_balancing_policy,
                   reconnection_policy,
                   retry_policy,
                   address_resolution_policy,
                   connector,
                   futures_factory,
                   timestamp_generator)
      @delegate_cluster = Cassandra::Cluster.new(logger,
                                                 io_reactor,
                                                 executor,
                                                 control_connection,
                                                 cluster_registry,
                                                 cluster_schema,
                                                 cluster_metadata,
                                                 execution_options,
                                                 connection_options,
                                                 load_balancing_policy,
                                                 reconnection_policy,
                                                 retry_policy,
                                                 address_resolution_policy,
                                                 connector,
                                                 futures_factory,
                                                 timestamp_generator)
      @graph_options = Dse::Graph::Options.new

      # We need the futures factory ourselves for async error reporting and potentially for our
      # own async processing independent of the C* driver.
      @futures = futures_factory
    end

    # Delegates to {http://datastax.github.io/ruby-driver/api/cassandra/cluster/#connect_async-instance_method
    # Cassandra::Cluster#connect_async}
    # to connect asynchronously to a cluster, but returns a future that will resolve to a DSE session rather than
    # Cassandra session.
    #
    # @param keyspace [String] optional keyspace to scope session to
    #
    # @return [Cassandra::Future<Dse::Session>]
    def connect_async(keyspace = nil)
      future = @delegate_cluster.connect_async(keyspace)
      # We want to actually return a DSE session upon successful connection.
      future.then do |cassandra_session|
        Dse::Session.new(cassandra_session, @graph_options, @futures)
      end
    end

    # Synchronous variant of {#connect_async}.
    #
    # @param keyspace [String] optional keyspace to scope the session to
    #
    # @return [Dse::Session]
    def connect(keyspace = nil)
      connect_async(keyspace).get
    end

    #### The following methods handle arbitrary delegation to the underlying cluster object. ####
    protected

    # @private
    def method_missing(method_name, *args, &block)
      # If we get here, we don't have a method of our own. Forward the request to the delegate_cluster.
      # If it returns itself, we will coerce the result to return our *self* instead.

      result = @delegate_cluster.send(method_name, *args, &block)
      (result == @delegate_cluster) ? self : result
    end

    # @private
    def respond_to?(method, include_private = false)
      super || @delegate_cluster.respond_to?(method, include_private)
    end
  end
end
