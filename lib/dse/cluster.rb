module Dse
  # Cluster represents a DSE cluster. It serves as a
  # {Dse::Session session factory} and a collection of metadata. It wraps
  # a {http://datastax.github.io/ruby-driver/api/cassandra/cluster Cassandra::Cluster} and exposes all of its
  # functionality.
  class Cluster
    # @private
    def initialize(*args)
      @delegate_cluster = Cassandra::Cluster.new(*args)
    end

    # Delegates to {http://datastax.github.io/ruby-driver/api/cassandra/cluster/#connect_async-instance_method
    # Cassandra::Cluster#connect_async}
    # to connect asynchronously to a cluster, but returns a future that will resolve to a DSE session rather than
    # Cassandra session.
    # @param options [Hash] (nil) connection options
    # @option options [String] :keyspace name of keyspace to scope session to for cql queries.
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
        Dse::Session.new(cassandra_session, Dse::Graph::Options.new(options))
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
