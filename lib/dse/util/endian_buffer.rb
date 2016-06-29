# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

module Dse
  module Util
    # Wrapper class around a Cassandra::Protocol::CqlByteBuffer that delegates reads of some
    # numeric values to the appropriate underlying method, depending on endian-ness.
    # @private
    class EndianBuffer
      def initialize(buffer, little_endian)
        @buffer = buffer
        # Depending on the endian-ness of the data, we want to invoke different read methods on the buffer.
        if little_endian
          @read_unsigned = buffer.method(:read_unsigned_int_le)
          @read_double = buffer.method(:read_double_le)
        else
          @read_unsigned = buffer.method(:read_int)
          @read_double = buffer.method(:read_double)
        end
      end

      def read_unsigned
        @read_unsigned.call
      end

      def read_double
        @read_double.call
      end
    end
  end
end
