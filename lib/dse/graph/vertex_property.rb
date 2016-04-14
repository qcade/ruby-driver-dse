# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

module Dse
  module Graph
    # Encapsulates a vertex-property complex value. The name of the property is stored in the owning
    # {Vertex}. This object contains a simple property value and any meta-properties that go with it.
    #
    # VertexProperty's are created when creating a Vertex object out of a graph query result hash. Access
    # to meta-properties is done via the {#properties} attribute, but there is also a short-cut using
    # array/hash dereference syntax:
    # @example
    #   val = vp.properties['meta1']
    #   # is the same as
    #   val = vp['meta1']
    class VertexProperty
      include Cassandra::Util

      # @return [Hash] id of this property
      attr_reader :id
      # @return [String] value of this property
      attr_reader :value
      # @return [Hash<String, String>] meta-properties of this property
      attr_reader :properties

      # @private
      def initialize(vertex_property_hash)
        # Vertex properties have three attributes: id, value, properties. Pull those out of the hash.
        @id = vertex_property_hash['id']
        @value = vertex_property_hash['value']
        @properties = vertex_property_hash.fetch('properties', {})
      end

      # @private
      def [](key)
        @properties[key]
      end

      # @private
      def eql?(other)
        # id's are unique among graph objects, so we only need to compare id's to test for equality.
        other.is_a?(VertexProperty) && \
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
        "#<Dse::Graph::VertexProperty:0x#{object_id.to_s(16)} " \
          "@id=#{@id.inspect}, " \
          "@value=#{@value.inspect}, " \
          "@properties=#{@properties.inspect}>"
      end
    end
  end
end
