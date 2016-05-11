# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

module Dse
  module Graph
    # Collection of results of running a graph query. It wraps a {Cassandra::Result}. When iterating
    # over results, individual results may be well-known domain objects or a generic {Result}.
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
      def_delegators :@results, :execution_info, :empty?, :size
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
      # @param ind [Integer] index into the collection of query results.
      def [](ind)
        @parsed_results[ind]
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
