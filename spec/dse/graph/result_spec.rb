# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

require 'spec_helper'

def vertex_hash(label, name, options = {})
  options = { community_id: 12345, member_id: 0, group_id: 5, properties: {} }.merge(options)
  result = {
    'id' => {
      'member_id' => options[:member_id],
      'community_id' => options[:community_id],
      '~label' => label,
      'group_id' => options[:group_id]
    },
    'label' => label,
    'type' => 'vertex',
    'properties' => {
      'name' => [
        {
          'id' => {
            'local_id' => '00000000-0000-8002-0000-000000000000',
            '~type' => 'name',
            'out_vertex' => {
              'member_id' => options[:member_id],
              'community_id' => options[:community_id],
              '~label' => label,
              'group_id' => options[:group_id]
            }
          },
          'value' => 'hercules'
        }
      ]
    }
  }

  # Add in any extra properties that were specified.
  options[:properties].each do |prop_name, value_list|
    prop_name = prop_name.to_s
    value_list = [value_list] unless value_list.is_a?(Array)

    value_list.each do |v|
      result['properties'][prop_name] ||= []
      prop_blob = {
        'id' => {
          'local_id' => '00000000-0000-8002-0000-000000000000',
          '~type' => prop_name,
          'out_vertex' => {
            'member_id' => options[:member_id],
            'community_id' => options[:community_id],
            '~label' => label,
            'group_id' => options[:group_id]
          }
        },
        'value' => v
      }
      prop_blob['properties'] = { 'k0' => 'v0' } if options[:include_meta]
      result['properties'][prop_name] << prop_blob
    end
  end

  result
end

def edge_hash(label, in_vertex_label, out_vertex_label, options = {})
  options = { community_id: 12345, member_id: 0, group_id: 5 }.merge(options)
  result = {
    'id' => {
      'out_vertex' => {
        'member_id' => options[:member_id],
        'community_id' => options[:community_id],
        '~label' => out_vertex_label,
        'group_id' => options[:group_id]
      },
      'local_id' => '27304f80-0050-11e6-9118-8188965167e5',
      'in_vertex' => {
        'member_id' => options[:member_id],
        'community_id' => options[:community_id],
        '~label' => in_vertex_label,
        'group_id' => options[:group_id]
      },
      '~type' => label
    },
    'label' => label,
    'type' => 'edge',
    'inVLabel' => in_vertex_label,
    'outVLabel' => out_vertex_label,
    'inV' => {
      'member_id' => options[:member_id],
      'community_id' => options[:community_id],
      '~label' => in_vertex_label,
      'group_id' => options[:group_id]
    },
    'outV' => {
      'member_id' => options[:member_id],
      'community_id' => options[:community_id],
      '~label' => out_vertex_label,
      'group_id' => options[:group_id]
    }
  }

  # Add in any extra properties that were specified.
  options[:properties].each do |prop_name, value|
    prop_name = prop_name.to_s
    result['properties'] ||= {}
    result['properties'][prop_name] = value
  end

  result
end

module Dse
  module Graph
    describe Result do
      it 'should handle vertex blobs' do
        r = Result.new(vertex_hash('demigod', 'hercules', properties: { age: 29 }))
        v = r.cast
        expect(v.class).to be(Vertex)
        expect(v.label).to eq('demigod')
        expect(v.id['community_id']).to eq(12345)
        expect(v.properties['name'][0].value).to eq('hercules')
        expect(v.properties['age'][0].value).to eq(29)
        expect(v.properties['age'][0].properties).to be_empty
        expect(v['name'][0].value).to eq('hercules')
        expect(v['age'][0].value).to eq(29)
      end

      it 'should handle vertex blobs with multi-valued properties' do
        r = Result.new(vertex_hash('demigod', 'hercules', properties: { friends: %w(aeolus xena) }))
        v = r.cast
        expect(v.class).to be(Vertex)
        expect(v.label).to eq('demigod')
        expect(v.id['community_id']).to eq(12345)
        expect(v.properties['name'][0].value).to eq('hercules')
        expect(v.properties['friends'][0].value).to eq('aeolus')
        expect(v.properties['friends'][1].value).to eq('xena')
        expect(v.properties['friends'][1].properties).to be_empty
        expect(v.properties['friends'].values).to eq(%w(aeolus xena))
      end

      it 'should handle vertex blobs with meta-properties' do
        r = Result.new(vertex_hash('demigod', 'hercules', properties: { age: 29 }, include_meta: true))
        v = r.cast
        expect(v.class).to be(Vertex)
        expect(v.label).to eq('demigod')
        expect(v.id['community_id']).to eq(12345)
        expect(v.properties['name'][0].value).to eq('hercules')
        expect(v.properties['age'][0].properties['k0']).to eq('v0')
        expect(v['age'][0]['k0']).to eq('v0')
      end

      it 'should handle edge blobs properly' do
        r = Result.new(edge_hash('father', 'god', 'titan', properties: { prop1: 'val1' }))
        e = r.cast
        expect(e.class).to be(Edge)
        expect(e.label).to eq('father')
        expect(e.in_v_label).to eq('god')
        expect(e.out_v_label).to eq('titan')
        expect(e.id['out_vertex']['community_id']).to eq(12345)
        expect(e.properties['prop1']).to eq('val1')
        expect(e['prop1']).to eq('val1')
      end

      it 'should handle simple results properly' do
        r = Result.new(7)
        expect(r.cast).to be(r)
        expect(r.value).to eq(7)
      end

      it 'should handle path results properly' do
        vertex1 = vertex_hash('demigod', 'hercules', properties: { age: 29 })
        vertex2 = vertex_hash('god', 'jupiter', properties: { age: 5000 })
        r = Result.new('labels' => [[], []], 'objects' => [vertex1, vertex2])

        # Path's aren't automatically deduced.
        expect(r.cast).to be(r)
        path = r.as_path
        expect(path.labels).to eq([[], []])
        expect(path.objects.size).to eq(2)
        expect(path.objects[0].class).to be(Vertex)
        expect(path.objects[0].label).to eq('demigod')
        expect(path.objects[1].class).to be(Vertex)
        expect(path.objects[1].label).to eq('god')
      end
    end
  end
end
