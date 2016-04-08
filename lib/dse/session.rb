module Dse
  # A session is used to execute queries. In addition to executing standard CQL queries
  # via the {http://datastax.github.io/ruby-driver/api/session/#execute-instance_method #execute} and
  # {http://datastax.github.io/ruby-driver/api/session/#execute_async-instance_method #execute_async} methods, it
  # executes graph queries via the {#execute_graph_async} and {#execute_graph} methods.
  #
  # @see http://datastax.github.io/ruby-driver/api/session Cassandra::Session
  class Session < ::Cassandra::Session
    # @private
    DEFAULT_GRAPH_OPTIONS = {
        graph_source: 'default',
        graph_language: 'gremlin-groovy'
    }

    # Execute a graph query synchronously.
    # @return [Cassandra::Result] a Cassandra result containing individual JSON results.
    def execute_graph(statement, options = nil)
      execute_graph_async(statement, options).get
    end

    # Execute a graph query asynchronously.
    # @param statement [String] a graph query
    # @param options [Hash] (nil) a customizable set of options
    #
    # @option options [Hash] :arguments Parameters for the graph query.
    # @option options [Hash] :graph_options Options for the DSE graph query handler. These include the following:
    #  :graph_name - name of the targeted graph
    #  :graph_source - graph traversal source
    #  :graph_language - language used in the query (default "gremlin-groovy")
    #  :graph_read_consistency_level - Read consistency level for graph query. Overrides the standard statement
    #     consistency level. Must be one of {Cassandra::CONSISTENCIES}
    #  :graph_write_consistency_level - Write consistency level for graph query. Overrides the standard statement
    #     consistency level. Must be one of {Cassandra::CONSISTENCIES}
    #
    # @return [Cassandra::Future<Cassandra::Result>]
    # @see Cassandra::Session::execute_async for all of the core options.
    def execute_graph_async(statement, options = nil)
      parameters = options.delete(:arguments)
      ::Cassandra::Util.assert_type(::Hash, parameters, 'Graph parameters must be a hash') unless parameters.nil?
      parameters = parameters.to_json

      graph_options = options[:graph_options].dup
      graph_options.merge!(DEFAULT_GRAPH_OPTIONS)
      payload = transform_graph_options(graph_options)

      p payload
      execute_async(statement, payload: payload)
    end

    private

    def transform_graph_options(graph_options)
      # Transform the user-provided graph options (which use symbols with _'s) into a hash with string keys,
      # where the keys are hyphenated (the way the server expects them).
      result = {}
      graph_options.each do |key, value|
        result[key.to_s.tr!('_', '-')] = value
      end
      result
    end
  end
end