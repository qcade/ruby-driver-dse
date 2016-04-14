# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

require 'spec_helper'

def vertex_property_blob(name, value, options = {})
  options = { community_id: 12345, member_id: 0, group_id: 5, vertex_label: 'vertex_label', properties: {} }
            .merge(options)
  result = {
    'id' => {
      'local_id' => '00000000-0000-8002-0000-000000000000',
      '~type' => name,
      'out_vertex' => {
        'member_id' => options[:member_id],
        'community_id' => options[:community_id],
        '~label' => options[:vertex_label],
        'group_id' => options[:group_id]
      }
    },
    'value' => value
  }

  # Add in meta-properties that were specified.
  options[:properties].each do |prop_name, prop_value|
    prop_name = prop_name.to_s
    result['properties'] ||= {}
    result['properties'][prop_name] = prop_value
  end

  result
end

module Dse
  module Graph
    describe VertexProperty do
      it 'should process property hash' do
        p = VertexProperty.new(vertex_property_blob('prop1', 'val1'))
        expect(p.value).to eq('val1')
        expect(p.id['out_vertex']['community_id']).to eq(12345)
        expect(p.properties['name']).to be_nil
        expect(p['name']).to be_nil
      end

      it 'should process meta-property hash' do
        p = VertexProperty.new(vertex_property_blob('prop1', 'val1', properties: {k1: 'v1', k2: 'v2'}))
        expect(p.value).to eq('val1')
        expect(p.id['out_vertex']['community_id']).to eq(12345)
        expect(p.properties['name']).to be_nil
        expect(p.properties['k1']).to eq('v1')
        expect(p['k1']).to eq('v1')
        expect(p['k2']).to eq('v2')
      end
    end
  end
end
