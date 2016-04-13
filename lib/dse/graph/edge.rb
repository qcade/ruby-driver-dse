# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

module Dse
  module Graph
    # Edge represents an edge in DSE graph. An edge connects two vertices.
    class Edge
      include Cassandra::Util

      # @return [Hash] id of this edge
      attr_reader :id
      # @return [String] label of this edge
      attr_reader :label
      # @return [Hash<String, String>] properties of this edge
      attr_reader :properties
      # @return [String] id of the "to" vertex of the edge
      attr_reader :inV
      # @return [String] label of the "to" vertex of the edge
      attr_reader :inVLabel
      # @return [String] id of the "from" vertex of the edge
      attr_reader :outV
      # @return [String] label of the "from" vertex of the edge
      attr_reader :outVLabel

      # @private
      def initialize(id, label, properties, inV, inVLabel, outV, outVLabel)
        # Validate args; this isn't an edge unless all args are non-nil.
        assert_not_empty(id, 'Cannot create Edge: id must not be empty')
        assert_not_empty(label, 'Cannot create Edge: label must not be empty')
        assert_not_empty(inV, 'Cannot create Edge: inV must not be empty')
        assert_not_empty(inVLabel, 'Cannot create Edge: inVLabel must not be empty')
        assert_not_empty(outV, 'Cannot create Edge: outV must not be empty')
        assert_not_empty(outVLabel, 'Cannot create Edge: outVLabel must not be empty')

        @id = id
        @label = label
        @properties = properties
        @inV = inV
        @inVLabel = inVLabel
        @outV = outV
        @outVLabel = outVLabel
      end

      # @private
      def [](key)
        @properties[key]
      end

      # @private
      def eql?(other)
        # id's are unique among graph objects, so we only need to compare id's to test for equality.
        other.is_a?(Edge) && \
        @id == other.id
      end

      # @private
      def hash
        # id's are unique among graph objects, so we only need to hash on the id for safely adding to a hash/set.
        @hash ||= 31 * 17 + @id.hash
      end

      # @private
      def inspect
        "#<Dse::Graph::Edge:0x#{object_id.to_s(16)} " \
          "@id=#{@id.inspect}, " \
          "@label=#{label.inspect}, " \
          "@properties=#{@properties.inspect}, " \
          "@inV=#{@inV.inspect}, " \
          "@inVLabel=#{@inVLabel.inspect}, " \
          "@outV=#{@outV.inspect}, " \
          "@outVLabel=#{@outVLabel.inspect}>"
      end
    end
  end
end
