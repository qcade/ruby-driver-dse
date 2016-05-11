# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

module Dse
  module Graph
    # An individual result of running a graph query. It wraps the JSON result and provides
    # access to it as a hash. It also supports casting the result into a known
    # domain object type: {Vertex}, {Edge}, and {Path} currently.
    #
    # @see ResultSet
    class Result
      # @return hash representation of the JSON result of a graph query if it's a complex result. A simple value
      #   otherwise
      attr_reader :value

      # @private
      def initialize(result)
        @value = result
      end

      # Coerce this result into a domain object if possible.
      # @return [Vertex, Edge, Result] a new wrapped object, or self if we can't cast it
      def cast
        type = @value['type'] if @value.is_a?(Hash)
        case type
        when 'vertex'
          as_vertex
        when 'edge'
          as_edge
        else
          self
        end
      end

      # Coerce this result into a {Vertex} object.
      # @return [Vertex] a vertex domain object
      # @raise [ArgumentError] if the result data does not represent a vertex
      def as_vertex
        Cassandra::Util.assert_instance_of(::Hash, @value)
        Dse::Graph::Vertex.new(@value['id'], @value['label'], @value.fetch('properties', {}))
      end

      # Coerce this result into an {Edge} object.
      # @return [Edge] an edge domain object.
      # @raise [ArgumentError] if the result data does not represent an edge.
      def as_edge
        Cassandra::Util.assert_instance_of(::Hash, @value)
        Dse::Graph::Edge.new(@value['id'], @value['label'], @value.fetch('properties', {}),
                             @value['inV'], @value['inVLabel'],
                             @value['outV'], @value['outVLabel'])
      end

      # Coerce this result into a {Path} object.
      # @return [Path] a path domain object.
      def as_path
        Cassandra::Util.assert_instance_of(::Hash, @value)
        Dse::Graph::Path.new(@value['labels'], @value['objects'])
      end

      # @private
      def eql?(other)
        other.is_a?(Result) && \
          @value == other.value
      end
      alias == eql?

      # @private
      def hash
        @hash ||= 31 * 17 + @value.hash
      end

      # @private
      def inspect
        "#<Dse::Graph::Result:0x#{object_id.to_s(16)} " \
          "@value=#{@value.inspect}>"
      end
    end
  end
end
