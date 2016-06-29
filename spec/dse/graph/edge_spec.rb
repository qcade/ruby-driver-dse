# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

require 'spec_helper'

module Dse
  module Graph
    describe Edge do
      let(:cons_args) do
        [{ id_attr: 1 }, 'label', { 'name' => 'myname' },
         { 'in_v_id' => 1 }, 'in_v_label',
         { 'out_v_id' => 2 }, 'out_v_label']
      end

      context :constructor do
        it 'should not yell if all args are specified' do
          Edge.new(*cons_args)
        end

        it 'should not yell if properties is blank' do
          cons_args[2] = {}
          Edge.new(*cons_args)
        end
      end
    end
  end
end
