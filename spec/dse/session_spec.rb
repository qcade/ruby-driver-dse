# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

require 'spec_helper'
require 'dse'

module Dse
  include Cassandra::Statements
  describe Session do
    let(:futures_factory) { double('futures-factory') }
    let(:future) { double('future') }
    let(:cassandra_session) { double('cassandra_session') }
    let(:session) { Session.new(cassandra_session, Dse::Graph::Options.new, futures_factory) }
    context :execute_graph_async do
      it 'should succeed without query parameters' do
        expected_graph_statement = Dse::Graph::Statement.new('g.V()', nil, session.graph_options)
        expect(cassandra_session).to receive(:execute_async)
          .with(expected_graph_statement, timeout: 32,
                payload: { 'graph-source' => 'g', 'graph-language' => 'gremlin-groovy' }).and_return(future)
        expect(future).to receive(:then)
        session.execute_graph_async('g.V()')
      end

      it 'should succeed with query parameters' do
        expected_graph_statement = Dse::Graph::Statement.new('g.V().limit(n)', {n: 2}, session.graph_options)
        expect(cassandra_session).to receive(:execute_async)
          .with(expected_graph_statement, arguments: { n: 2 }, timeout: 32,
                                  payload: { 'graph-source' => 'g', 'graph-language' => 'gremlin-groovy' })
          .and_return(future)
        expect(future).to receive(:then)
        session.execute_graph_async('g.V().limit(n)', arguments: { n: 2 })
      end

      it 'should error out if parameters are not a hash' do
        expect(futures_factory).to receive(:error).with(instance_of(ArgumentError))
        session.execute_graph_async('g.V().limit(n)', arguments: 7)
      end

      it 'should accept graph options hash' do
        execution_options = {graph_source: 'other', graph_name: 'myg', random: 'junk'}
        expected_graph_statement = Dse::Graph::Statement.new('g.V()', nil, Dse::Graph::Options.new(execution_options))
        expect(cassandra_session).to receive(:execute_async)
          .with(expected_graph_statement,
                execution_options.merge(
                payload: { 'graph-source' => 'other', 'graph-language' => 'gremlin-groovy', 'graph-name' => 'myg' },
                timeout: 32))
          .and_return(future)
        expect(future).to receive(:then)
        options = Dse::Graph::Options.new
        options.graph_source = 'other'
        options.graph_name = 'myg'
        session.execute_graph_async('g.V()', execution_options)
      end

      it 'should accept graph options object' do
        options = Dse::Graph::Options.new
        options.graph_source = 'other'
        options.graph_name = 'myg'
        expected_graph_statement = Dse::Graph::Statement.new('g.V()', nil, options)
        expect(cassandra_session).to receive(:execute_async)
          .with(expected_graph_statement, graph_options: options, timeout: 32,
                                  payload: { 'graph-source' => 'other',
                                             'graph-language' => 'gremlin-groovy',
                                             'graph-name' => 'myg' })
          .and_return(future)
        expect(future).to receive(:then)
        session.execute_graph_async('g.V()', graph_options: options)
      end

      it 'should accept graph statement object' do
        graph_statement = Dse::Graph::Statement.new('g.V()', nil, { graph_name: 'myg' }, true)
        expect(cassandra_session).to receive(:execute_async)
          .with(graph_statement,
                random: 'junk', timeout: 32,
                payload: { 'graph-source' => 'g', 'graph-language' => 'gremlin-groovy', 'graph-name' => 'myg' })
          .and_return(future)
        expect(future).to receive(:then)
        session.execute_graph_async(graph_statement, random: 'junk')
      end

      it 'should error out if options is not a hash nor Options' do
        expect(futures_factory).to receive(:error).with(instance_of(ArgumentError))
        session.execute_graph_async('g.V()', graph_options: 7)
      end
    end
  end
end
