# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

module Dse
  module Graph
    # Vertex represents a vertex in DSE graph. Vertices have sophisticated properties. A given property can have
    # multiple values, and each value can have a collection of meta-properties. To access a property of a vertex, you'd
    # reference the {#properties} hash, and then get the n'th value, which is a {VertexProperty}, and then get the
    # actual value from there.
    # @example To get the first value of the 'name' property:
    #   v.properties['name'][0].value
    # @example Use array/hash dereference shortcut
    #   v['name'][0].value
    # @example Get all of the values of the 'name' property
    #   values = v['name'].map do |vertex_prop|
    #     vertex_prop.value
    #   end
    # @example Use the 'values' method on the array to do the heavy-lifting for you.
    #   values = v['name'].values
    # @example VertexProperty exposes meta-properties for a value:
    #   meta1 = v['name'][0].properties['meta1']
    #   # Shortcut
    #   meta1 = v['name'][0]['meta1']
    class Vertex
      include Cassandra::Util

      # @return [Hash] id of this vertex.
      attr_reader :id
      # @return [String] label of this vertex.
      attr_reader :label
      # @return [Hash<String, Array<VertexProperty>>] properties of this vertex.
      attr_reader :properties

      # @private
      def initialize(id, label, properties)
        # Validate args; this isn't an edge unless all args are non-nil.
        assert_not_empty(id, 'Cannot create Vertex: id must not be empty')
        assert_not_empty(label, 'Cannot create Vertex: label must not be empty')
        @id = id
        @label = label

        # Vertex properties are structured like this:
        # { 'name' => [
        #     { 'id' => {... }, 'value' => 'some_val', 'properties' => { 'key' => 'value' } },
        #     { 'id' => {... }, 'value' => 'some_val2', 'properties' => { 'key2' => 'value2' } }
        # ], 'age' => [........], .... }
        #
        # When storing in @properties, we convert it to use VertexProperty's:
        # { 'name' => [
        #     vertex_prop1,
        #     vertex_prop2
        # ], 'age' => [........], .... }
        #
        # With that structure, we'll support syntactic sugar that lets users get to properties and meta-properties
        # more easily:
        #
        # v['name'][0].value  -- get the first value of the 'name' property.
        # v['name'][0]['key'] -- get the value of the 'key' meta-property of the first name property.

        @properties = {}
        properties.each do |prop_name, values|
          vertex_props = values.map do |v|
            VertexProperty.new(v)
          end

          # Add a 'values' method to the array to get the values of each prop
          # in one array.
          def vertex_props.values
            self.map do |v|
              v.value
            end
          end

          @properties[prop_name] = vertex_props
        end
      end

      # @private
      def [](key)
        @properties[key]
      end

      # @private
      def eql?(other)
        # id's are unique among graph objects, so we only need to compare id's to test for equality.
        other.is_a?(Vertex) && \
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
        "#<Dse::Graph::Vertex:0x#{object_id.to_s(16)} " \
          "@id=#{@id.inspect}, " \
          "@label=#{label.inspect}, " \
          "@properties=#{@properties.inspect}, " \
          "@type=#{@type.inspect}>"
      end
    end
  end
end
