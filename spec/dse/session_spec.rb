# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

require 'spec_helper'

module Dse
  describe Session do
    let(:future) { double('future') }
    context :execute_graph_async do
      it 'should succeed without query parameters' do
        cassandra_session = double('cassandra_session')
        session = Session.new(cassandra_session)
        expect(cassandra_session).to receive(:execute_async).with('g.V()', {
            payload: { "graph-source" => "default", "graph-language" => "gremlin-groovy" }
        }).and_return(future)
        expect(future).to receive(:then)
        session.execute_graph_async('g.V()')
      end

      it 'should succeed with query parameters' do
        cassandra_session = double('cassandra_session')
        session = Session.new(cassandra_session)
        expect(cassandra_session).to receive(:execute_async).with('g.V().limit(n)', {
            arguments: ['{"n":2}'],
            payload: { "graph-source" => "default", "graph-language" => "gremlin-groovy" } }).
            and_return(future)
        expect(future).to receive(:then)
        session.execute_graph_async('g.V().limit(n)', arguments: { n: 2 })
      end

      it 'should error out if parameters are not a hash' do
        cassandra_session = double('cassandra_session')
        session = Session.new(cassandra_session)
        expect { session.execute_graph_async('g.V().limit(n)', arguments: 7) }.to raise_error(ArgumentError)
      end

      it 'should accept graph options' do
        cassandra_session = double('cassandra_session')
        session = Session.new(cassandra_session)
        expect(cassandra_session).to receive(:execute_async).with('g.V()', {
            payload: { "graph-source" => "other", "graph-language" => "gremlin-groovy", 'graph-name' => 'myg' } }).
            and_return(future)
        expect(future).to receive(:then)
        session.execute_graph_async('g.V()', graph_options: { graph_source: 'other', graph_name: 'myg' })
      end
    end
  end
end