# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

# A load balancing policy that returns hosts in sorted order
class OrderedPolicy < Cassandra::LoadBalancing::Policy
  class Plan
    def initialize(hosts)
      @hosts = hosts.sort { |a,b| a.ip.to_s <=> b.ip.to_s }
    end

    def has_next?
      @hosts.size > 0
    end

    def next
      host = @hosts.first
      @hosts.delete(host)
      host
    end
  end

  extend Forwardable

  def_delegators :@base_policy, :distance, :host_found, :host_lost

  def initialize(base_policy)
    @base_policy = base_policy
    @hosts = []
  end

  def host_up(host)
    @hosts << host
    @base_policy.host_up(host)
  end

  def host_down(host)
    @hosts.delete(host)
    @base_policy.host_down(host)
  end

  def setup(cluster)
  end

  def teardown(cluster)
  end

  def plan(keyspace, statement, options)
    Plan.new(@hosts)
  end
end
