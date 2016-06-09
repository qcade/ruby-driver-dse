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

      # @private
      WKT_RE = /^POINT\s*\(\s*([^)]+?)\s*\)$/
      POINT_SPEC_RE = /^([0-9\-\.]+) ([0-9\-\.]+)$/

      # @param args [Array<Numeric>,Array<String>] varargs-style arguments in two forms:
      #   <ul><li>a two-element numeric array representing x,y coordinates.</li>
      #       <li>one-element string array with the wkt representation.</li></ul>
      #
      # @example Construct a Point with numeric arguments.
      #   point = Point.new(3, 4)
      # @example Construct a Point with a wkt string.
      #   point = Point.new('POINT (3.0 4.0)')
      def initialize(*args)
        # The constructor has two forms:
        # 1. two numeric args (x,y)
        # 2. one String arg as the wkt representation.

        case args.size
        when 2
          x, y = args
          Cassandra::Util.assert_instance_of(::Numeric, x)
          Cassandra::Util.assert_instance_of(::Numeric, y)
          Cassandra::Util.assert(!x.nan?, 'x cannot be Float::NAN') if x.is_a?(Float)
          Cassandra::Util.assert(!y.nan?, 'y cannot be Float::NAN') if y.is_a?(Float)
          @x = x.to_f
          @y = y.to_f
        when 1
          wkt = args.first
          Cassandra::Util.assert_instance_of(String, wkt)
          match = wkt.match(WKT_RE)
          raise ArgumentError, "#{wkt.inspect} is not a valid WKT representation of a point" unless match
          @x, @y = self.class.parse_wkt_internal(match[1])
        else
          raise ArgumentError,
                'wrong number of arguments: use one string argument (wkt) or two numeric arguments (x,y)'
        end
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
      def self.parse_wkt_internal(point_str)
        point_str.rstrip!
        match = point_str.match(POINT_SPEC_RE)
        raise ArgumentError, "#{point_str.inspect} is not a valid WKT representation of a point" unless match
        [match[1].to_f, match[2].to_f]
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
