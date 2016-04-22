# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

module Dse
  # Cluster represents a DSE cluster. It serves as a
  # {Dse::Session session factory} and a collection of metadata. It wraps
  # a {http://datastax.github.io/ruby-driver/api/cassandra/cluster Cassandra::Cluster} and exposes all of its
  # functionality.
  class Cluster
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
                   futures_factory)
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
                                                 futures_factory)
      # We need the futures factory ourselves for async error reporting and potentially for our
      # own async processing independent of the C* driver.
      @futures = futures_factory
    end

    # Delegates to {http://datastax.github.io/ruby-driver/api/cassandra/cluster/#connect_async-instance_method
    # Cassandra::Cluster#connect_async}
    # to connect asynchronously to a cluster, but returns a future that will resolve to a DSE session rather than
    # Cassandra session.
    # @param options [Hash] (nil) connection options
    # @option options [String] :keyspace name of keyspace to scope session to for cql queries.
    # @option options [Dse::Graph::Options] :graph_options options for the DSE graph statement handler. Takes
    #    priority over other `:graph_*` options specified below.
    # @option options [String] :graph_name name of graph to use in graph statements
    # @option options [String] :graph_source graph traversal source
    # @option options [String] :graph_alias alias to use for the graph traversal object in graph statements
    # @option options [String] :graph_language language used in graph queries
    # @option options [Cassandra::CONSISTENCIES] :graph_read_consistency read consistency level for graph statements.
    #    Overrides the standard statement consistency level
    # @option options [Cassandra::CONSISTENCIES] :graph_write_consistency write consistency level for graph statements.
    #    Overrides the standard statement consistency level
    # @return [Cassandra::Future<Dse::Session>]
    def connect_async(options = {})
      future = @delegate_cluster.connect_async(options[:keyspace])
      # We want to actually return a DSE session upon successful connection.
      future.then do |cassandra_session|
        graph_options = if !options[:graph_options].nil?
                          Cassandra::Util.assert_instance_of(Dse::Graph::Options, options[:graph_options])
                          options[:graph_options]
                        else
                          Dse::Graph::Options.new(options)
                        end
        Dse::Session.new(cassandra_session, graph_options, @futures)
      end
    end

    # Synchronous variant of {#connect_async}.
    # @return [Dse::Session]
    def connect(*args)
      connect_async(*args).get
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
