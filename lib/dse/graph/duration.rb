# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

module Dse
  module Graph
    # Represents a duration of time, corresponding to the Duration datatype in DSE Graph.
    class Duration
      attr_reader :days, :hours, :minutes, :seconds

      # @private
      # We expect a string of the form PnDTnHnMn.nS, where n's are positive or negative integers, and where
      # components may be missing (e.g. PT7.8S is valid)
      PAT = /^P((?<days>-?\d+)D)?T((?<hours>-?\d+)H)?((?<minutes>-?\d+)M)?((?<seconds>-?[0-9.]+)S)?$/

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

      def self.from_dse(duration)
        parse_result = PAT.match(duration.to_s)
        raise(ArgumentError,
              "Failed to parse '#{duration}': expected format PnDTnHnMn.nS with integer n" \
              ' and optionally missing duration components') unless parse_result
        Duration.new(parse_result[:days], parse_result[:hours], parse_result[:minutes], parse_result[:seconds])
      end

      # @private
      def to_s
        # Construct a string of the form PnDTnHnMn.nS
        "P#{@days}DT#{@hours}H#{@minutes}M#{@seconds}S"
      end

      # @private
      def eql?(other)
        other.is_a?(Duration) && \
          @days == other.days && \
          @hours == other.hours && \
          @minutes == other.minutes && \
          @seconds == other.seconds
      end
      alias == eql?

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
