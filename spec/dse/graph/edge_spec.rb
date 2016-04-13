# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

require 'spec_helper'

module Dse
  module Graph
    describe Edge do
      let(:cons_args) {
        [{ id_attr: 1 }, 'label', { 'name' => 'myname' },
         { 'inV_id' => 1 }, 'inVLabel',
         { 'outV_id' => 2 }, 'outVLabel']
      }

      context :constructor do
        it 'should not yell if all args are specified' do
          Edge.new(*cons_args)
        end

        it "should not yell if properties is blank" do
          cons_args[2] = {}
          Edge.new(*cons_args)
        end

        ['id', 'label', nil, 'inV', 'inVLabel', 'outV', 'outVLabel'].each_with_index do |arg_name, ind|
          next if arg_name.nil?
          it "should yell if #{arg_name} is blank" do
            cons_args[ind] = cons_args[ind].is_a?(Hash) ? {} : ''
            expect { Edge.new(*cons_args) }.to raise_error(ArgumentError)
          end
        end
      end
    end
  end
end