# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

module Dse
  module LoadBalancing
    module Policies
      # A load balancing policy that targets a particular host, and delegates to a lower-level policy if the
      # host is not available.
      # @private
      class HostTargeting < Cassandra::LoadBalancing::Policy
        # @private
        class Plan
          def initialize(targeted_host, policy, keyspace, statement, options)
            @targeted_host = targeted_host
            @policy = policy
            @keyspace = keyspace
            @statement = statement
            @options = options
            @first = true
          end

          def has_next?
            @next = @targeted_host if @first && !@targeted_host.nil? && @targeted_host.up?
            @first = false
            return true if @next

            @plan ||= @policy.plan(@keyspace, @statement, @options)

            while @plan.has_next?
              host = @plan.next

              unless host == @targeted_host
                @next = host
                return true
              end
            end

            false
          end

          def next
            host = @next
            @next = nil
            host
          end
        end

        extend Forwardable

        # @!method distance(host)
        #   Delegates to wrapped policy
        #   @see Cassandra::LoadBalancing::Policy#distance
        #
        # @!method host_found(host)
        #   Delegates to wrapped policy
        #   @see Cassandra::LoadBalancing::Policy#host_found
        #
        # @!method host_up(host)
        #   Delegates to wrapped policy
        #   @see Cassandra::LoadBalancing::Policy#host_up
        #
        # @!method host_down(host)
        #   Delegates to wrapped policy
        #   @see Cassandra::LoadBalancing::Policy#host_down
        #
        # @!method host_lost(host)
        #   Delegates to wrapped policy
        #   @see Cassandra::LoadBalancing::Policy#host_lost
        def_delegators :@base_policy, :distance, :host_found, :host_up, :host_down, :host_lost

        # @param base_policy [Cassandra::LoadBalancing::Policy] policy to delegate to if host is not available.
        def initialize(base_policy)
          @base_policy = base_policy
        end

        def setup(cluster)
          @cluster = cluster
          @base_policy.setup(cluster)
          nil
        end

        def teardown(cluster)
          @cluster = nil
          @base_policy.teardown(cluster)
          nil
        end

        def plan(keyspace, statement, options)
          # Fall back to creating a plan from the base policy if the statement is not host-targeting.
          return @base_policy.plan(keyspace, statement, options) unless statement.is_a?(Dse::Statements::HostTargeting)
          Plan.new(@cluster.host(statement.target_ip), @base_policy, keyspace, statement, options)
        end
      end
    end
  end
end
