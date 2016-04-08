module Dse
  # Cluster represents a DSE cluster. It serves as a
  # {Dse::Session session factory} and a collection of metadata. It wraps
  # a {http://datastax.github.io/ruby-driver/api/cluster Cassandra::Cluster} and exposes all of its functionality.
  class Cluster < ::Cassandra::Cluster
    # @private
    def new_session(client)
      Dse::Session.new(client, @execution_options, @futures)
    end
  end
end