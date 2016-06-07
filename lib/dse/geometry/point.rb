# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

module Dse
  module Geometry
    # Encapsulates a 2D point with x,y coordinates. It corresponds to the `org.apache.cassandra.db.marshal.PointType`
    # column type in DSE.
    #
    # @see https://en.wikipedia.org/wiki/Well-known_text Wikipedia article on Well Known Text
    class Point
      include Cassandra::CustomData

      # @return [Float] the x coordinate of the point.
      attr_reader :x
      # @return [Float] the y coordinate of the point.
      attr_reader :y

      # @param x [Float] the x coordinate of the point.
      # @param y [Float] the y coordinate of the point.
      def initialize(x, y)
        Cassandra::Util.assert_instance_of(::Numeric, x)
        Cassandra::Util.assert_instance_of(::Numeric, y)
        @x = x.to_f
        @y = y.to_f
      end

      # @return [String] well-known-text representation of this point.
      def wkt
        "POINT (#{wkt_internal})"
      end

      # @private
      def wkt_internal
        # This is a helper used to embed point coords into some container (e.g. line-string, polygon)
        "#{@x} #{@y}"
      end

      # @private
      def eql?(other)
        other.is_a?(Point) && \
          @x == other.x && \
          @y == other.y
      end
      alias == eql?

      # @private
      def hash
        @hash ||= begin
          h = 17
          h = 31 * h + @x.hash
          h = 31 * h + @y.hash
          h
        end
      end

      # @private
      def inspect
        "#<Point:0x#{object_id.to_s(16)} " \
          "@x=#{@x.inspect}, " \
          "@y=#{@y.inspect}>"
      end

      # @private
      def to_s
        "#{@x},#{@y}"
      end

      # methods related to serializing/deserializing.

      # @private
      TYPE = Cassandra::Types::Custom.new('org.apache.cassandra.db.marshal.PointType')

      # @return [Cassandra::Types::Custom] type of column that is processed by this domain object class.
      def self.type
        TYPE
      end

      # Deserialize the given data into an instance of this domain object class.
      # @param data [String] byte-array representation of a column value of this custom type.
      # @return [Point]
      # @raise [Cassandra::Errors::DecodingError] upon failure.
      def self.deserialize(data)
        buffer = Cassandra::Protocol::CqlByteBuffer.new(data)
        little_endian = buffer.read(1) != "\x00"

        # Depending on the endian-ness of the data, we want to read it differently. Wrap the buffer
        # with an "endian-aware" reader that reads the desired way.
        buffer = Dse::Util::EndianBuffer.new(buffer, little_endian)

        type = buffer.read_unsigned
        raise Cassandra::Errors::DecodingError, "Point data-type value should be 1, but was #{type}" if type != 1
        deserialize_raw(buffer)
      end

      # This is a helper function to deserialize the meat of the data (after we've accounted for endianness
      # and other metadata)
      # @private
      def self.deserialize_raw(buffer)
        x = buffer.read_double
        y = buffer.read_double
        Point.new(x, y)
      end

      # Serialize this domain object into a byte array to send to DSE.
      # @return [String] byte-array representation of this domain object.
      def serialize
        buffer = Cassandra::Protocol::CqlByteBuffer.new

        # We can serialize according to our native platform, but it's just as easy to lock into an endian. We
        # choose big-endian because the Cassandra protocol is big-endian and we definitely have all the methods
        # we need to write out such values.

        buffer << "\x00"

        # This is a point.
        buffer.append_int(1)

        # Write out x and y.
        serialize_raw(buffer)

        buffer
      end

      # This is a helper function to serialize the meat of the data (after we've accounted for endianness
      # and other metadata)
      # @private
      def serialize_raw(buffer)
        buffer.append_double(@x)
        buffer.append_double(@y)
      end
    end
  end
end
