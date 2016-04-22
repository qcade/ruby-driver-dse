# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

require 'spec_helper'
require 'dse'

module Dse
  module LoadBalancing
    module Policies
      describe HostTargeting do
        let (:base_policy) { double('base_policy') }
        let (:statement) { double('statement') }
        let (:host) {double('host')}
        let (:host2) {double('host2')}
        let (:base_plan) {double('base_plan')}
        let (:cluster) {double('cluster')}
        let (:policy) do
          policy = HostTargeting.new(base_policy)
          policy.setup(cluster)
          policy
        end

        before do
          expect(base_policy).to receive(:setup)
        end

        it 'should return the target host if it is up' do
          # Here's the series of events we expect:
          # When we call policy.plan, we'll check that the statement is HostTargeting and
          #  get the target_ip from the statement, then get the Host object from :cluster,
          #  then create a Plan (inner class).
          # When we call plan.has_next?, the plan will realize this is the first time and that the host is up,
          #  and thus return true. :next will return the host that's referenced in the statement.
          expect(base_policy).to_not receive(:plan)
          expect(statement).to receive(:is_a?).and_return(true)
          expect(statement).to receive(:target_ip).and_return('127.0.0.1')
          expect(cluster).to receive(:host).with('127.0.0.1').and_return(host)
          expect(host).to receive(:up?).and_return(true)

          plan = policy.plan('ks', statement, nil)
          expect(plan.has_next?).to eq(true)
          expect(plan.next).to be(host)
        end

        it 'should delegate to base policy for second host' do
          # See flow in first test. Once that's done, calling has_next? again will
          # ignore the targeted-host and delegate to the underlying policy to create a plan.
          # We configure the base-plan to not have any hosts.
          expect(base_policy).to receive(:plan).and_return(base_plan)
          expect(statement).to receive(:is_a?).and_return(true)
          expect(statement).to receive(:target_ip).and_return('127.0.0.1')
          expect(cluster).to receive(:host).with('127.0.0.1').and_return(host)
          expect(host).to receive(:up?).and_return(true)
          expect(base_plan).to receive(:has_next?).and_return(false)

          plan = policy.plan('ks', statement, nil)
          expect(plan.has_next?).to eq(true)
          expect(plan.next).to be(host)
          expect(plan.has_next?).to eq(false)
        end

        it 'should skip targeted-host in base plan' do
          # This is a lot like the first test also, but this time we configure the
          # base plan to return two hosts: host and host2. Since host is the targeted-host,
          # it should be skipped.
          expect(base_policy).to receive(:plan).and_return(base_plan)
          expect(statement).to receive(:is_a?).and_return(true)
          expect(statement).to receive(:target_ip).and_return('127.0.0.1')
          expect(cluster).to receive(:host).with('127.0.0.1').and_return(host)
          expect(host).to receive(:up?).and_return(true)
          expect(base_plan).to receive(:has_next?).and_return(true, true, false)
          expect(base_plan).to receive(:next).and_return(host, host2)

          plan = policy.plan('ks', statement, nil)
          expect(plan.has_next?).to eq(true)
          expect(plan.next).to be(host)
          expect(plan.has_next?).to eq(true)
          expect(plan.next).to be(host2)
          expect(plan.has_next?).to eq(false)
        end

        it 'should not return the target host if it is down' do
          # Like the first test, except the targeted-host is down, so we delegate to
          # the base policy immediately.
          expect(base_policy).to receive(:plan).and_return(base_plan)
          expect(statement).to receive(:is_a?).and_return(true)
          expect(statement).to receive(:target_ip).and_return('127.0.0.1')
          expect(cluster).to receive(:host).with('127.0.0.1').and_return(host)
          expect(host).to receive(:up?).and_return(false)
          expect(base_plan).to receive(:has_next?).and_return(false)

          plan = policy.plan('ks', statement, nil)
          expect(plan.has_next?).to eq(false)
        end

        it 'should return a plan from the base policy if the statement is not host-targeting' do
          expect(statement).to receive(:is_a?).and_return(false)
          expect(base_policy).to receive(:plan).and_return(base_plan)
          expect(statement).to_not receive(:target_ip)

          plan = policy.plan('ks', statement, nil)
          expect(plan).to be(base_plan)
        end

        it 'should delegate to base plan if there is no target-host' do
          expect(statement).to receive(:is_a?).and_return(true)
          expect(statement).to receive(:target_ip).and_return(nil)
          expect(cluster).to receive(:host).with(nil).and_return(nil)
          expect(base_policy).to receive(:plan).and_return(base_plan)
          expect(base_plan).to receive(:has_next?).and_return(false)

          plan = policy.plan('ks', statement, nil)
          expect(plan.has_next?).to eq(false)
        end
      end
    end
  end
end
