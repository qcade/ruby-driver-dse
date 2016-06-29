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
      end
    end
  end
end
