# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
      attr_reader :in_v
      # @return [String] label of the "to" vertex of the edge
      attr_reader :in_v_label
      # @return [String] id of the "from" vertex of the edge
      attr_reader :out_v
      # @return [String] label of the "from" vertex of the edge
      attr_reader :out_v_label

      # @private
      def initialize(id, label, properties, in_v, in_v_label, out_v, out_v_label)
        @id = id
        @label = label
        @properties = properties
        @in_v = in_v
        @in_v_label = in_v_label
        @out_v = out_v
        @out_v_label = out_v_label
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
      alias == eql?

      # @private
      def hash
        # id's are unique among graph objects, so we only need to hash on the id for safely adding to a hash/set.
        @hash ||= 31 * 17 + @id.hash
      end

      # @private
      def inspect
        "#<Dse::Graph::Edge:0x#{object_id.to_s(16)} " \
          "@id=#{@id.inspect}, " \
          "@label=#{@label.inspect}, " \
          "@properties=#{@properties.inspect}, " \
          "@in_v=#{@in_v.inspect}, " \
          "@in_v_label=#{@in_v_label.inspect}, " \
          "@out_v=#{@out_v.inspect}, " \
          "@out_v_label=#{@out_v_label.inspect}>"
      end
    end
  end
end
