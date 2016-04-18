# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

require 'spec_helper'

module Dse
  module Graph
    include Cassandra::Statements
    describe Statement do
      let(:basic_graph_options) { Dse::Graph::Options.new(graph_name: 'mygraph') }
      context :constructor do
        it 'should work with minimal arguments' do
          statement = Statement.new('g.V()', nil)
          expect(statement.statement).to eq('g.V()')
          expect(statement.parameters).to be_nil
          expect(statement.options).to be_nil
          expect(statement.simple_statement).to eq(Simple.new('g.V()', nil, nil, false))
        end

        it 'should respect idempotent option' do
          statement = Statement.new('g.V()', nil, nil, true)
          expect(statement.statement).to eq('g.V()')
          expect(statement.parameters).to be_nil
          expect(statement.options).to be_nil
          expect(statement.simple_statement).to eq(Simple.new('g.V()', nil, nil, true))
        end

        it 'should work with parameters' do
          statement = Statement.new('g.V().limit(m)', m: 3)
          expect(statement.statement).to eq('g.V().limit(m)')
          expect(statement.parameters).to eq(m: 3)
          expect(statement.options).to be_nil
          expect(statement.simple_statement)
            .to eq(Simple.new('g.V().limit(m)', ['{"m":3}'], [Cassandra::Types.varchar], false))
        end

        it 'should fail if parameters is not a hash' do
          expect { Statement.new('g.V().limit(m)', [1]) }.to raise_error(ArgumentError)
        end

        it 'should work with options being a Graph Options object' do
          options = basic_graph_options
          statement = Statement.new('g.V()', nil, options)
          expect(statement.statement).to eq('g.V()')
          expect(statement.parameters).to be_nil
          expect(statement.options).to be(options)
          expect(statement.simple_statement).to eq(Simple.new('g.V()', nil, nil, false))
        end

        it 'should work with options being a hash containing a graph_options key' do
          options = {graph_options: basic_graph_options, random_option: 'value'}
          statement = Statement.new('g.V()', nil, options)
          expect(statement.statement).to eq('g.V()')
          expect(statement.parameters).to be_nil
          expect(statement.options).to be(basic_graph_options)
          expect(statement.simple_statement).to eq(Simple.new('g.V()', nil, nil, false))
        end

        it 'should work with options being a hash containing graph_* keys' do
          options = {graph_name: 'mygraph', random_option: 'value'}
          statement = Statement.new('g.V()', nil, options)
          expect(statement.statement).to eq('g.V()')
          expect(statement.parameters).to be_nil
          expect(statement.options).to eq(basic_graph_options)
          expect(statement.simple_statement).to eq(Simple.new('g.V()', nil, nil, false))
        end

        it 'should fail if options is not a hash nor Options' do
          expect { Statement.new('g.V().limit(m)', {m: 3}, ['illegal options']) }.to raise_error(ArgumentError)
        end
      end
    end
  end
end
