# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

module Dse
  module Geometry
    # Encapsulates a polygon consisting of a set of linear-rings in the xy-plane. It corresponds to the
    # `org.apache.cassandra.db.marshal.PolygonType` column type in DSE.
    #
    # A linear-ring is a {LineString} whose last point is the same as its first point. The first ring specified
    # in a polygon defines the outer edges of the polygon and is called the _exterior ring_. A polygon may also have
    # _holes_ within it, specified by other linear-rings, and those holes may contain linear-rings indicating
    # _islands_. All such rings are called _interior rings_.
    #
    # @see https://en.wikipedia.org/wiki/Well-known_text Wikipedia article on Well Known Text
    class Polygon
      include Cassandra::CustomData

      # @param rings [Array<LineString>] ordered collection of linear-rings that make up this polygon.
      def initialize(rings)
        @rings = rings.freeze
      end

      # @return [LineString] linear-ring characterizing the exterior of the polygon.
      def exterior_ring
        @rings.first
      end

      # @return [Array<LineString>] ordered collection of linear-rings that make up the interior of this polygon.
      def interior_rings
        @interior_rings ||= @rings[1..-1].freeze
      end

      # @return [String] well-known-text representation of this polygon.
      def wkt
        result = 'POLYGON ('
        first = true
        @rings.each do |ring|
          result += ', ' unless first
          first = false
          result += "(#{ring.wkt_internal})"
        end
        result += ')'
        result
      end

      # @private
      def eql?(other)
        other.is_a?(Polygon) && \
          @rings == other.instance_variable_get(:@rings)
      end
      alias == eql?

      # @private
      def hash
        @hash ||= 31 * 17 + @rings.hash
      end

      # @private
      def inspect
        "#<Polygon:0x#{object_id.to_s(16)} " \
          "@exterior_ring=#{@rings.first.inspect}, " \
        "@interior_rings=#{interior_rings.inspect}>"
      end

      # @private
      def to_s
        "Exterior ring: #{@rings.first}\n" \
          "Interior rings:\n    " +
          interior_rings.join("\n    ")
      end

      # methods related to serializing/deserializing.

      # @private
      TYPE = Cassandra::Types::Custom.new('org.apache.cassandra.db.marshal.PolygonType')

      # @return [Cassandra::Types::Custom] type of column that is processed by this domain object class.
      def self.type
        TYPE
      end

      # Deserialize the given data into an instance of this domain object class.
      # @param data [String] byte-array representation of a column value of this custom type.
      # @return [Polygon]
      # @raise [Cassandra::Errors::DecodingError] upon failure.
      def self.deserialize(data)
        buffer = Cassandra::Protocol::CqlByteBuffer.new(data)
        little_endian = buffer.read(1) != "\x00"

        # Depending on the endian-ness of the data, we want to read it differently. Wrap the buffer
        # with an "endian-aware" reader that reads the desired way.
        buffer = Dse::Util::EndianBuffer.new(buffer, little_endian)

        type = buffer.read_unsigned
        raise Cassandra::Errors::DecodingError, "LineString data-type value should be 3, but was #{type}" if type != 3

        # Now comes the number of rings.
        num_rings = buffer.read_unsigned

        # Read that many line-string's (rings) from the buffer.
        rings = []
        num_rings.times do
          rings << LineString.deserialize_raw(buffer)
        end
        Polygon.new(rings)
      end

      # Serialize this domain object into a byte array to send to DSE.
      # @return [String] byte-array representation of this domain object.
      def serialize
        buffer = Cassandra::Protocol::CqlByteBuffer.new

        # We can serialize according to our native platform, but it's just as easy to lock into an endian. We
        # choose big-endian because the Cassandra protocol is big-endian and we definitely have all the methods
        # we need to write out such values.

        buffer << "\x00"

        # This is a polygon.
        buffer.append_int(3)

        # Write out the count of how many rings we have.
        buffer.append_int(@rings.size)

        # Now write out the raw serialization of each ring (e.g. linestring).
        @rings.each do |ring|
          ring.serialize_raw(buffer)
        end

        buffer
      end
    end
  end
end
