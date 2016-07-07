# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

module Dse
  module Graph
    # @!parse
    #   class Options
    #     # @return [String] name of the targeted graph; required unless the statement is a system query.
    #     attr_accessor :graph_name
    #     # @return [String] graph traversal source (default "g")
    #     attr_accessor :graph_source
    #     # @return [String] language used in the graph statement (default "gremlin-groovy")
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
        'graph-source' => 'g',
        'graph-language' => 'gremlin-groovy'
      }.freeze

      # @private
      OPTION_NAMES = [
        :graph_name,
        :graph_source,
        :graph_language,
        :graph_read_consistency,
        :graph_write_consistency,
        :request_timeout
      ].freeze

      # Create an Options object.
      # @param options [Hash] optional hash containing graph options. Keys are option name symbols
      #    (e.g. `:graph_name`). Unset options will inherit from the defaults.
      def initialize(options = {})
        # Filter the given options to only those we care about.
        @real_options = {}
        return unless options

        options.each do |k, v|
          set(k, v) if OPTION_NAMES.include?(k)
        end

        @real_options['request-timeout'] = [options[:timeout] * 1000].pack("Q>") if options[:timeout]
      end

      OPTION_NAMES.each do |attr|
        define_method(attr.to_s) do
          @real_options[stringify(attr)]
        end

        define_method("#{attr}=") do |value|
          @real_options[stringify(attr)] = value
        end
      end

      # Set an option in this {Options} object. This is primarily used to set "expert" options that
      # are not part of the public api and thus may change over time.
      # @param key [String, Symbol] option to set.
      # @param value [String] value to set for the option.
      # @return [Options] self, thus allowing method chaining.
      def set(key, value)
        @real_options[stringify(key)] = value if value
        self
      end

      # Merge another {Options} object with this one to produce a new merged {Options} object.
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

      # @private
      def stringify(attr)
        attr.to_s.tr('_', '-')
      end

      # @private
      def merge!(other)
        result = merge(other)
        @real_options = result.instance_variable_get(:@real_options)
        self
      end

      # @private
      def clear
        # Used by tests only.
        @real_options.clear
      end

      # @return whether or not this options object is configured for the analytics graph source.
      def analytics?
        @real_options['graph-source'] == 'a'
      end

      # @private
      def as_payload
        # Merge in real options with defaults to get a final payload
        DEFAULT_GRAPH_OPTIONS.merge(@real_options)
      end

      # @private
      def eql?(other)
        other.is_a?(Options) && \
          @real_options == other.instance_variable_get(:@real_options)
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
