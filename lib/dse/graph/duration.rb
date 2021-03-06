# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

module Dse
  module Graph
    # Represents a duration of time, corresponding to the Duration datatype in DSE Graph. In DSE Graph,
    # this type is represented by the
    # {https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html Java 8 Duration} type.
    class Duration
      # Days in duration of time.  May be negative. Is internally coerced to an integer,
      # so a value being assigned need not be an `Integer` itself.
      # @return [Integer] days in duration of time.
      attr_reader :days

      # Hours in duration of time.  May be negative. Is internally coerced to an integer,
      # so a value being assigned need not be an `Integer` itself.
      # @return [Integer] hours in duration of time.
      attr_reader :hours

      # Minutes in duration of time.  May be negative. Is internally coerced to an integer,
      # so a value being assigned need not be an `Integer` itself.
      # @return [Integer] minutes in duration of time.
      attr_reader :minutes

      # Seconds in duration of time.  May be negative. Is internally coerced to an float,
      # so a value being assigned need not be an `Float` itself.
      # @return [Float] seconds in duration of time.
      attr_reader :seconds

      # @private
      # We expect a string of the form PnDTnHnMn.nS, where n's are positive or negative integers, and where
      # components may be missing (e.g. PT7.8S is valid)
      PAT = /^P((?<days>-?\d+)D)?T((?<hours>-?\d+)H)?((?<minutes>-?\d+)M)?((?<seconds>-?[0-9.]+)S)?$/

      # Create a {Duration} object. All arguments are internally coerced to desired types.
      # @param days [Integer] number of days in the time-frame. May be negative.
      # @param hours [Integer] number of hours in the time-frame. May be negative.
      # @param minutes [Integer] number of minutes in the time-frame. May be negative.
      # @param seconds [Float] number of seconds in the time-frame. May be negative.
      def initialize(days, hours, minutes, seconds)
        @days = days.to_i
        @hours = hours.to_i
        @minutes = minutes.to_i
        @seconds = seconds.to_f
      end

      def days=(days)
        @days = days.to_i
      end

      def hours=(hours)
        @hours = hours.to_i
      end

      def minutes=(minutes)
        @minutes = minutes.to_i
      end

      def seconds=(seconds)
        @seconds = seconds.to_f
      end

      # Parse a duration string from DSE Graph and construct a {Duration} object
      # @param duration [String] duration string from DSE Graph.
      # @raise [ArgumentError] if the duration string fails to parse.
      def self.parse(duration)
        parse_result = PAT.match(duration.to_s)
        raise(ArgumentError,
              "Failed to parse '#{duration}': expected format PnDTnHnMn.nS with integer n" \
              ' and optionally missing duration components') unless parse_result
        Duration.new(parse_result[:days], parse_result[:hours], parse_result[:minutes], parse_result[:seconds])
      end

      # A string formatted as `PnDTnHnMn.nS`, where `n` is a number that goes with the character code following it.
      #
      # D - days<br>
      # H - hours<br>
      # M - minutes<br>
      # S - seconds<br>
      #
      # @example a duration of 1 day, 2 hours, 3 minutes, 4.5 seconds
      #  P1DT2H3M4.5S
      # @return [String] this {Duration} as a string formatted `PnDTnHnMn.nS`.
      #
      # @see https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-
      #   Java 8 Duration#parse
      def to_s
        # Construct a string of the form PnDTnHnMn.nS
        "P#{@days}DT#{@hours}H#{@minutes}M#{@seconds}S"
      end

      # @private
      def eql?(other)
        other.is_a?(Duration) && as_seconds == other.as_seconds
      end
      alias == eql?

      # @return [Float] this {Duration} object converted to seconds
      def as_seconds
        @seconds + @minutes * 60 + @hours * 3600 + @days * 86400
      end

      # @private
      def hash
        @hash ||= begin
          h = 17
          h = 31 * h + @days.hash
          h = 31 * h + @hours.hash
          h = 31 * h + @minutes.hash
          h = 31 * h + @seconds.hash
          h
        end
      end

      # @private
      def inspect
        "#<Duration:0x#{object_id.to_s(16)} " \
          "@days=#{@days.inspect}, " \
          "@hours=#{@hours.inspect}, " \
          "@minutes=#{@minutes.inspect}, " \
          "@seconds=#{@seconds.inspect}>"
      end
    end
  end
end
