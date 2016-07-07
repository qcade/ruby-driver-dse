# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

require 'spec_helper'

module Dse
  module Auth
    module Providers
      describe GssApi do
        context :constructor do
          let(:custom_resolver) do
            x = Object.new
            def x.resolve(host)
              'fake'
            end
            x
          end

          it 'should default to a NameInfoResolver' do
            provider = GssApi.new('foo')
            expect(provider.instance_variable_get(:@host_resolver)).to be_instance_of(GssApi::NameInfoResolver)
          end

          it 'should treat false to be NoOpResolver' do
            provider = GssApi.new('foo', false)
            expect(provider.instance_variable_get(:@host_resolver)).to be_instance_of(GssApi::NoOpResolver)
          end

          it 'should treat true to be NameInfoResolver' do
            provider = GssApi.new('foo', true)
            expect(provider.instance_variable_get(:@host_resolver)).to be_instance_of(GssApi::NameInfoResolver)
          end

          it 'should accept custom host resolver' do
            provider = GssApi.new('foo', custom_resolver)
            expect(provider.instance_variable_get(:@host_resolver)).to be(custom_resolver)
          end

          it 'should reject custom host resolver that does not implement resolve' do
            expect { GssApi.new('foo', Object.new) }.to raise_error(ArgumentError)
            expect { GssApi.new('foo', :foo) }.to raise_error(ArgumentError)
          end
        end
      end
    end
  end
end
