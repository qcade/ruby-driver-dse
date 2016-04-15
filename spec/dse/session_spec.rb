# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

require 'spec_helper'

module Dse
  describe Session do
    let(:future) { double('future') }
    let(:cassandra_session) { double('cassandra_session') }
    let(:session) { Session.new(cassandra_session, Dse::Graph::Options.new) }
    context :execute_graph_async do
      it 'should succeed without query parameters' do
        expect(cassandra_session).to receive(:execute_async)
          .with('g.V()',
                payload: { 'graph-source' => 'default', 'graph-language' => 'gremlin-groovy' }).and_return(future)
        expect(future).to receive(:then)
        session.execute_graph_async('g.V()')
      end

      it 'should succeed with query parameters' do
        expect(cassandra_session).to receive(:execute_async)
          .with('g.V().limit(n)', arguments: ['{"n":2}'],
                                  payload: { 'graph-source' => 'default', 'graph-language' => 'gremlin-groovy' })
          .and_return(future)
        expect(future).to receive(:then)
        session.execute_graph_async('g.V().limit(n)', arguments: { n: 2 })
      end

      it 'should error out if parameters are not a hash' do
        expect { session.execute_graph_async('g.V().limit(n)', arguments: 7) }.to raise_error(ArgumentError)
      end

      it 'should accept graph options hash' do
        expect(cassandra_session).to receive(:execute_async)
          .with('g.V()',
                payload: { 'graph-source' => 'other', 'graph-language' => 'gremlin-groovy', 'graph-name' => 'myg' })
          .and_return(future)
        expect(future).to receive(:then)
        options = Dse::Graph::Options.new
        options.graph_source = 'other'
        options.graph_name = 'myg'
        session.execute_graph_async('g.V()',
                                    graph_options: { graph_source: 'other', graph_name: 'myg', random: 'junk' })
      end

      it 'should accept graph options object' do
        expect(cassandra_session).to receive(:execute_async)
          .with('g.V()',
                payload: { 'graph-source' => 'other', 'graph-language' => 'gremlin-groovy', 'graph-name' => 'myg' })
          .and_return(future)
        expect(future).to receive(:then)
        options = Dse::Graph::Options.new
        options.graph_source = 'other'
        options.graph_name = 'myg'
        session.execute_graph_async('g.V()', graph_options: options)
      end

      it 'should error out if options is not a hash nor Options' do
        expect { session.execute_graph_async('g.V()', graph_options: 7) }.to raise_error(ArgumentError)
      end
    end
  end
end
