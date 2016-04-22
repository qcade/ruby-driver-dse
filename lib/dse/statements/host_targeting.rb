# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

module Dse
  module Statements
    # Wraps any query statement and attaches a target host, making it usable in a targeted load-balancing policy
    # without modifying the user's statement.
    # @private
    class HostTargeting
      include Cassandra::Util
      include Cassandra::Statement

      # @return  the base statement to execute.
      attr_reader :base_statement
      # @return [String] the ip address of the host on which the statement should run if possible.
      attr_reader :target_ip

      def initialize(base_statement, target_ip)
        @base_statement = base_statement
        @target_ip = target_ip
      end

      # @private
      def accept(client, options)
        client.query(self, options)
      end

      def idempotent?
        @base_statement.idempotent?
      end

      protected

      def method_missing(method_name, *args, &block)
        # Delegate all method calls to the real statement that we're wrapping.
        @base_statement.send(method_name, *args, &block)
      end

      def respond_to?(method, include_private = false)
        super || @base_statement.respond_to?(method, include_private)
      end
    end
  end
end

