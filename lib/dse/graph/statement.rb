# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

module Dse
  module Graph
    # Encapsulates a graph statement, parameters, options, and idempotency. This is primarily useful for
    # re-issuing the same statement multiple times the same way.
    class Statement
      include Cassandra::Statement

      # @return [String] graph statement string
      attr_reader :statement
      # @return [Hash<String, String>] parameters to the statement
      attr_reader :parameters
      # @return [Options] graph options
      attr_reader :options
      # @private
      attr_reader :simple_statement

      # @param statement [String] graph statement
      # @param parameters [Hash<String, String>] (nil) parameters to the statement
      # @param options [Hash, Options] (nil) graph options
      # @param idempotent [Boolean] (false) whether or not the statement is idempotent
      def initialize(statement, parameters = nil, options = nil, idempotent = false)
        # Save off statement and idempotent; easy stuff.
        @statement = statement.freeze
        @idempotent = idempotent.freeze
        @parameters = parameters.freeze

        # Convert the parameters into a one-element array with JSON; that's what we need to
        # send to DSE over the wire. But if we have no params, nil is fine.
        unless parameters.nil?
          ::Cassandra::Util.assert_instance_of(::Hash, parameters, 'Graph parameters must be a hash')
          # Some of our parameters may be geo-type values. Convert them to their wkt representation.
          tweaked_params = {}
          parameters.each do |name, value|
            value = value.wkt if value.respond_to?(:wkt)
            tweaked_params[name] = value
          end
          parameters = [tweaked_params.to_json]
        end

        # Graph options may be tricky. A few cases:
        # 1. options is nil; then @options should be nil.
        # 2. options is a Hash with a graph_options key; then @options is the referenced Options object.
        #    We must validate that the referenced object *is* an Options.
        # 3. options is an Options object; then just assign to @options.

        unless options.nil?
          Cassandra::Util.assert_instance_of_one_of([Dse::Graph::Options, ::Hash], options)
          @options = if options.is_a?(Options)
                       options
                     elsif !options[:graph_options].nil?
                       Cassandra::Util.assert_instance_of(Dse::Graph::Options, options[:graph_options])
                       options[:graph_options]
                     else
                       Dse::Graph::Options.new(options)
                     end
        end

        @simple_statement = Cassandra::Statements::Simple.new(@statement,
                                                              parameters,
                                                              parameters.nil? ? nil : [Cassandra::Types.varchar],
                                                              @idempotent)
      end

      # @private
      def accept(client, options)
        simple_statement.accept(client, options)
      end

      # @private
      def eql?(other)
        other.is_a?(Statement) && \
          @statement == other.statement && \
          @parameters == other.parameters && \
          @options == other.options && \
          @idempotent == other.idempotent?
      end
      alias == eql?

      # @private
      def hash
        @hash ||= begin
          h = 17
          h = 31 * h + @statement.hash
          h = 31 * h + @parameters.hash
          h = 31 * h + @options.hash
          h = 31 * h + @idempotent.hash
          h
        end
      end

      # @private
      def inspect
        "#<Dse::Graph::Statement:0x#{object_id.to_s(16)} " \
          "@statement=#{@statement.inspect}, " \
          "@parameters=#{@parameters.inspect}, " \
          "@options=#{@options.inspect}, " \
          "@idempotent=#{@idempotent.inspect}>"
      end

      protected

      # @private
      def method_missing(method, *args, &block)
        # Delegate unrecognized method calls to our embedded statement.
        @simple_statement.send(method, *args, &block)
      end
    end
  end
end
