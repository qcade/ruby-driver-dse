# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

require 'spec_helper'

module Dse
  module Graph
    describe Vertex do
      let(:cons_args) { [{ id_attr: 1 }, 'label', { 'name' => [{'id' => {}, 'value' => 'myname'}] }] }

      context :constructor do
        it 'should not yell if all args are specified' do
          Vertex.new(*cons_args)
        end

        it 'should not yell if properties is blank' do
          cons_args[2] = {}
          Vertex.new(*cons_args)
        end

        %w(id label).each_with_index do |arg_name, ind|
          it "should yell if #{arg_name} is blank" do
            cons_args[ind] = cons_args[ind].is_a?(Hash) ? {} : ''
            expect { Vertex.new(*cons_args) }.to raise_error(ArgumentError)
          end
        end
      end
    end
  end
end
