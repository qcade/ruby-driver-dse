# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

module Dse
  module Geometry
    # Encapsulates a set of lines, characterized by a sequence of {Point}s in the xy-plane. It corresponds to the
    # `org.apache.cassandra.db.marshal.LineStringType` column type in DSE.
    #
    # @see https://en.wikipedia.org/wiki/Well-known_text Wikipedia article on Well Known Text
    class LineString
      include Cassandra::CustomData

      # @return [Array<Point>] collection of points that make up this line-string.
      attr_reader :points

      # @private
      WKT_RE = /^LINESTRING\s*\(\s*([^)]+)\s*\)$/
      POINT_SEPARATOR_RE = /\s*,\s*/

      # @param args [Array<Point>,Array<String>] varargs-style arguments in two forms:
      #   <ul><li>ordered collection of points that make up this line-string.
      #           Must be empty or have at least two points.</li>
      #       <li>one-element string array with the wkt representation.</li></ul>
      #
      # @example Construct an empty LineString
      #   line = LineString.new
      # @example Construct a LineString with Point objects.
      #   line = LineString.new(Point.new(1.0, 2.0), Point.new(3.0, 4.0))
      # @example Construct a LineString with a wkt string.
      #   line = LineString.new('LINESTRING (1.0 2.0, 3.0 4.0)')
      def initialize(*args)
        # The constructor has two forms:
        # 1. 0, 2, or more Point objects in the order in which one connects them to make a line-string.
        # 2. one String arg as the wkt representation.

        if args.size == 1
          wkt = args.first
          Cassandra::Util.assert_instance_of(String, wkt)

          if wkt == 'LINESTRING EMPTY'
            @points = [].freeze
          else
            match = wkt.match(WKT_RE)
            raise ArgumentError, "#{wkt.inspect} is not a valid WKT representation of a line-string" unless match
            @points = self.class.parse_wkt_internal(match[1])
          end
        else
          @points = args.freeze
          @points.each do |p|
            Cassandra::Util.assert_instance_of(Point, p, "#{p.inspect} is not a Point")
          end
        end
      end

      # @private
      def self.parse_wkt_internal(line_str)
        point_strings = line_str.split(POINT_SEPARATOR_RE)
        points = []
        point_strings.each do |ps|
          next if ps.empty?
          points << Point.new(*Point.parse_wkt_internal(ps))
        end
        points.freeze
      end

      # @return [String] well-known-text representation of this line-string.
      def wkt
        @points.empty? ? 'LINESTRING EMPTY' : "LINESTRING (#{wkt_internal})"
      end

      # @private
      def wkt_internal
        # This is a helper used to embed point coords into some container (e.g. polygon)
        @points.map(&:wkt_internal).join(', ')
      end

      # @private
      def eql?(other)
        other.is_a?(LineString) && \
          @points == other.points
      end
      alias == eql?

      # @private
      def hash
        @hash ||= 31 * 17 + @points.hash
      end

      # @private
      def inspect
        "#<LineString:0x#{object_id.to_s(16)} " \
          "@points=#{@points.inspect}>"
      end

      # @private
      def to_s
        @points.join(' to ')
      end

      # methods related to serializing/deserializing.

      # @private
      TYPE = Cassandra::Types::Custom.new('org.apache.cassandra.db.marshal.LineStringType')

      # @return [Cassandra::Types::Custom] type of column that is processed by this domain object class.
      def self.type
        TYPE
      end

      # Deserialize the given data into an instance of this domain object class.
      # @param data [String] byte-array representation of a column value of this custom type.
      # @return [LineString]
      # @raise [Cassandra::Errors::DecodingError] upon failure.
      def self.deserialize(data)
        buffer = Cassandra::Protocol::CqlByteBuffer.new(data)
        little_endian = buffer.read(1) != "\x00"

        # Depending on the endian-ness of the data, we want to read it differently. Wrap the buffer
        # with an "endian-aware" reader that reads the desired way.
        buffer = Dse::Util::EndianBuffer.new(buffer, little_endian)

        type = buffer.read_unsigned
        raise Cassandra::Errors::DecodingError, "LineString data-type value should be 2, but was #{type}" if type != 2

        deserialize_raw(buffer)
      end

      # This is a helper function to deserialize the meat of the data (after we've accounted for endianness
      # and other metadata)
      # @private
      def self.deserialize_raw(buffer)
        # Now comes the number of points in the line-string.
        num_points = buffer.read_unsigned

        # Read that many x,y coords from the buffer.
        points = []
        num_points.times do
          points << Point.deserialize_raw(buffer)
        end
        LineString.new(*points)
      end

      # Serialize this domain object into a byte array to send to DSE.
      # @return [String] byte-array representation of this domain object.
      def serialize
        buffer = Cassandra::Protocol::CqlByteBuffer.new

        # We can serialize according to our native platform, but it's just as easy to lock into an endian. We
        # choose big-endian because the Cassandra protocol is big-endian and we definitely have all the methods
        # we need to write out such values.

        buffer << "\x00"

        # This is a line-string.
        buffer.append_int(2)

        # Write out the count of how many points we have.
        serialize_raw(buffer)

        buffer
      end

      # This is a helper function to serialize the meat of the data (after we've accounted for endianness
      # and other metadata)
      # @private
      def serialize_raw(buffer)
        buffer.append_int(@points.size)

        # Now write out x and y for each point.
        @points.each do |point|
          point.serialize_raw(buffer)
        end
      end
    end
  end
end
