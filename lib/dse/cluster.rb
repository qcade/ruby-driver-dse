require 'forwardable'

module Dse
  # Cluster represents a DSE cluster. It serves as a
  # {Dse::Session session factory} and a collection of metadata. It wraps
  # a {http://datastax.github.io/ruby-driver/api/cluster Cassandra::Cluster} and exposes all of its functionality.
  class Cluster
    extend Forwardable

    def_delegators :@delegate_cluster, :register, :unregister, :each_host, :hosts, :each_keyspace, :keyspaces,
                   :refresh_schema_async, :refresh_schema,
                   :name, :find_replicas,
                   :SEVERAL_MORE_SYMBOLS_SOME_OF_WHICH_ARE_DELEGATES_THEMSELVES_IN_CLUSTER_LIKE_NAME_AND_FIND_REPLICAS

    def initialize(*args)
      @delegate_cluster = Cassandra::Cluster.new(*args)
    end

    def connect_async(*args)
      # TODO
    end

    def connect(*args)
      # TODO
    end

    # Many cluster methods return "self". So all of those methods need to be implemented here
    # to delegate to the embedded cluster and then return *our* self.
    # With some dynamic method creation logic, we can probably do that efficiently, but
    # it feels like we're going to a lot of trouble to make Dse::Cluster act like a
    # Cassandra::Cluster without actually being a Cassandra::Cluster.
    # Several of the entries in def_delegators above are in this category, and don't belong in def_delegators.


    # @private
    def new_session(client)
      Dse::Session.new(client, @execution_options, @futures)
    end
  end
end