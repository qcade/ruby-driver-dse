# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

module Dse
  module Graph
    # @!parse
    #   class Options
    #     # @return [String] name of the targeted graph; required unless the statement is a system query.
    #     attr_accessor :graph_name
    #     # @return [String] graph traversal source (default "default")
    #     attr_accessor :graph_source
    #     # @return [String] alias to use for the graph traversal object (default "g")
    #     attr_accessor :graph_language
    #     # @return [Cassandra::CONSISTENCIES] read consistency level for graph statement.
    #     #   Overrides the standard statement consistency level. Defaults to ONE in the server,
    #     #   but the default may be configured differently.
    #     attr_accessor :graph_read_consistency
    #     # @return [Cassandra::CONSISTENCIES] write consistency level for graph statement.
    #     #   Overrides the standard statement consistency level. Defaults to QUORUM in the server,
    #     #   but the default may be configured differently.
    #     attr_accessor :graph_write_consistency
    #   end

    # Options for DSE Graph queries
    class Options
      # @private
      DEFAULT_GRAPH_OPTIONS = {
        graph_source: 'default',
        graph_language: 'gremlin-groovy'
      }.freeze

      # @private
      OPTION_NAMES = [
        :graph_name,
        :graph_source,
        :graph_language,
        :graph_read_consistency,
        :graph_write_consistency
      ].freeze

      # Create an Options object.
      # @param options [Hash] optional hash containing graph options. Keys are attribute name symbols
      #    (e.g. :graph_name). Unset options will inherit from the defaults.
      def initialize(options = {})
        # Filter the given options to only those we care about.
        @real_options = options.select do |key, _|
          OPTION_NAMES.include?(key)
        end
      end

      OPTION_NAMES.each do |attr|
        define_method(attr.to_s) do
          @real_options[attr]
        end

        define_method("#{attr}=") do |value|
          @real_options[attr] = value
        end
      end

      # Merge another Options object with this one to produce a new merged Options object.
      # The "other" object's values take precedence over this one.
      # @param other [Options] Options object to merge with this one.
      # @return [Options] new Options object with the merged options.
      def merge(other)
        # Just return our-self (no need to copy) if we're merging in nothing.
        return self if other.nil?

        # This is fairly efficient, but manipulates the guts of an Options object.
        result = Options.new
        result.instance_variable_set(:@real_options,
                                     @real_options.merge(other.instance_variable_get(:@real_options)))
        result
      end

      # @return whether or not this options object is configured for the analytics graph source.
      def is_analytics?
        @real_options[:graph_source] == 'a'
      end

      # @private
      def as_payload
        graph_options = DEFAULT_GRAPH_OPTIONS.merge(@real_options)
        # Transform the graph options (which use symbols with _'s) into a hash with string keys,
        # where the keys are hyphenated (the way the server expects them).
        result = {}
        graph_options.each do |key, value|
          result[key.to_s.tr!('_', '-')] = value if value
        end
        result
      end

      # @private
      def eql?(other)
        other.is_a?(Options) && \
          @real_options[:graph_name] == other.graph_name && \
          @real_options[:graph_source] == other.graph_source && \
          @real_options[:graph_language] == other.graph_language && \
          @real_options[:graph_read_consistency] == other.graph_read_consistency && \
          @real_options[:graph_write_consistency] == other.graph_write_consistency
      end
      alias == eql?

      # @private
      def hash
        # id's are unique among graph objects, so we only need to hash on the id for safely adding to a hash/set.
        @hash ||= 31 * 17 + @real_options.hash
      end

      # @private
      def inspect
        "#<Dse::Graph::Options:0x#{object_id.to_s(16)} " \
        "@graph_name=#{@real_options[:graph_name].inspect}, " \
        "@graph_source=#{@real_options[:graph_source].inspect}, " \
        "@graph_language=#{@real_options[:graph_language].inspect}, " \
        "@graph_read_consistency=#{@real_options[:graph_read_consistency].inspect}, " \
        "@graph_write_consistency=#{@real_options[:graph_write_consistency].inspect}>"
      end
    end
  end
end
