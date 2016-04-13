module Dse
  # Cluster represents a DSE cluster. It serves as a
  # {Dse::Session session factory} and a collection of metadata. It wraps
  # a {http://datastax.github.io/ruby-driver/api/cassandra/cluster Cassandra::Cluster} and exposes all of its functionality.
  class Cluster
    # @private
    def initialize(*args)
      @delegate_cluster = Cassandra::Cluster.new(*args)
    end

    # Delegates to {http://datastax.github.io/ruby-driver/api/cassandra/cluster/#connect_async-instance_method Cassandra::Cluster#connect_async}
    # to connect asynchronously to a cluster, but returns a future that will resolve to a DSE session rather than
    # Cassandra session.
    # @return [Cassandra::Future<Dse::Session>]
    def connect_async(*args)
      future = @delegate_cluster.connect_async(*args)
      # We want to actually return a DSE session upon successful connection.
      future.then do |cassandra_session|
        Dse::Session.new(cassandra_session)
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