# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

module Dse
  module Graph
    # Collection of results of running a graph query. It wraps a {Cassandra::Result}. When iterating
    # over results, individual results may be well-known domain objects or a generic {Result}.
    #
    # A ResultSet is actually a page of results. Use the {#next_page} or {#next_page_async} method to retrieve a
    # new result-set for the next page (if any).
    #
    # @see Vertex
    # @see Edge
    # @see Path
    # @see Result
    class ResultSet
      include Enumerable
      extend Forwardable

      # @private -- just for ==/eql?
      attr_reader :parsed_results

      # @!method execution_info
      #   Query execution information, such as number of retries and all tried hosts, etc.
      #   @return [Cassandra::Execution::Info]
      #
      # @!method empty?
      #   @return [Boolean] whether it has any result data
      #
      # @!method size
      #   @return [Integer] number of results
      #
      # @!method last_page?
      #   @return [Boolean] whether more pages are available
      def_delegators :@results, :execution_info, :empty?, :size, :last_page?
      alias length size

      # @private
      def initialize(results)
        @results = results
        @parsed_results = results.map do |r|
          Result.new(JSON.parse(r.fetch('gremlin', {})).fetch('result', {})).cast
        end
      end

      # @yieldparam result [Vertex, Edge, Path, Result] result object for a particular result
      # @return [Enumerator, self] returns Enumerator if no block given
      def each(&block)
        @parsed_results.each(&block)
      end

      # Allow array indexing into the result-set.
      # @note As a ResultSet represents a page of results, this indexing is into the current page.
      # @param ind [Integer] index into the collection of query results.
      def [](ind)
        @parsed_results[ind]
      end

      # Loads next page asynchronously
      #
      # @param options [Hash] additional options, just like the ones for
      #   {Dse::Session#execute_graph_async}
      #
      # @note `:paging_state` option will be ignored.
      #
      # @return [Cassandra::Future<Dse::Graph::ResultSet>] a future that resolves to a new ResultSet if there is a new
      #   page, `nil` otherwise.
      #
      # @see Dse::Session#execute_graph_async
      def next_page_async(options = nil)
        @results.next_page_async(options).then do |raw_results|
          ResultSet.new(raw_results)
        end
      end

      # Loads next page synchronously
      # @see #next_page_async
      # @return [Dse::Graph::ResultSet, nil] returns `nil` if last page
      def next_page(options = nil)
        next_page_async(options).get
      end

      # @private
      def eql?(other)
        other.is_a?(ResultSet) && \
          @parsed_results == other.parsed_results
      end
      alias == eql?

      # @private
      def hash
        @hash ||= 31 * 17 + @parsed_results.hash
      end

      # @private
      def inspect
        "#<Dse::ResultSet:0x#{object_id.to_s(16)} []=#{@parsed_results.inspect}>"
      end
    end
  end
end
