module Dse
  # A session is used to execute queries. In addition to executing standard CQL queries
  # via the {http://datastax.github.io/ruby-driver/api/cassandra/session/#execute-instance_method #execute} and
  # {http://datastax.github.io/ruby-driver/api/cassandra/session/#execute_async-instance_method #execute_async}
  # methods, it executes graph queries via the {#execute_graph_async} and {#execute_graph} methods.
  #
  # @see http://datastax.github.io/ruby-driver/api/cassandra/session Cassandra::Session
  class Session
    # @return [Dse::Graph::Options] default graph options used by queries on this session.
    attr_reader :graph_options

    # @private
    def initialize(cassandra_session, graph_options)
      @cassandra_session = cassandra_session
      @graph_options = graph_options
    end

    # Execute a graph query asynchronously.
    # @param statement [String] a graph query
    # @param options [Hash] a customizable set of options. All of the options supported by
    #   {Cassandra::Session#execute_async} are valid here. However, there are some extras, noted below.
    # @option options [Hash] :arguments Parameters for the graph query.
    #    NOTE: Unlike {#execute} and {#execute_async}, this must be a hash of &lt;parameter-name,value>.
    # @option options [Dse::Graph::Options] :graph_options options for the DSE graph query handler. Takes
    #    priority over other `:graph_*` options specified below.
    # @option options [String] :graph_name name of graph to use in graph queries
    # @option options [String] :graph_source graph traversal source
    # @option options [String] :graph_alias alias to use for the graph traversal object in graph queries
    # @option options [String] :graph_language language used in graph queries
    # @option options [Cassandra::CONSISTENCIES] :graph_read_consistency read consistency level for graph queries.
    #    Overrides the standard statement consistency level
    # @option options [Cassandra::CONSISTENCIES] :graph_write_consistency write consistency level for graph queries.
    #    Overrides the standard statement consistency level
    # @return [Cassandra::Future<Cassandra::Result>]
    # @see http://datastax.github.io/ruby-driver/api/cassandra/session/#execute_async-instance_method
    #   Cassandra::Session::execute_async for all of the core options.
    def execute_graph_async(statement, options = {})
      # Make our own copy of the options. The caller might want to re-use the options they provided, and we're
      # about to do some destructive mutations/massages.

      options = options.dup
      parameters = options.delete(:arguments)

      unless parameters.nil?
        ::Cassandra::Util.assert_instance_of(::Hash, parameters, 'Graph parameters must be a hash')
        options[:arguments] = [parameters.to_json]
      end

      graph_options = options.delete(:graph_options)
      if graph_options.nil?
        # Since an Options object wasn't given to us, construct it from the options hash.
        graph_options = Dse::Graph::Options.new(options)
      end
      Cassandra::Util.assert_instance_of(Dse::Graph::Options, graph_options)

      options[:payload] = @graph_options.merge(graph_options).as_payload

      @cassandra_session.execute_async(statement, options).then do |raw_result|
        Dse::Graph::ResultSet.new(raw_result)
      end
    end

    # Execute a graph query synchronously.
    # @see #execute_graph_async
    # @return [Cassandra::Result] a Cassandra result containing individual JSON results.
    def execute_graph(statement, options = {})
      execute_graph_async(statement, options).get
    end

    # @return [String] the name of the graph that graph queries are bound to by default in this session.
    def graph_name
      @graph_options.graph_name
    end

    #### The following methods handle arbitrary delegation to the underlying session object. ####
    protected

    # @private
    def method_missing(method_name, *args, &block)
      # If we get here, we don't have a method of our own. Forward the request to @cassandra_session.
      # If it returns itself, we will coerce the result to return our *self* instead.

      result = @cassandra_session.send(method_name, *args, &block)
      (result == @cassandra_session) ? self : result
    end

    # @private
    def respond_to?(method, include_private = false)
      super || @cassandra_session.respond_to?(method, include_private)
    end
  end
end
