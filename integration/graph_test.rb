# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#++

require File.dirname(__FILE__) + '/integration_test_case.rb'

class GraphTest < IntegrationTestCase
  def self.before_suite
    if CCM.dse_version < '5.0.0'
      puts "DSE > 5.0 required for graph tests, skipping setup."
    else
      @@ccm_cluster = CCM.setup_graph_cluster(1, 3)

      @@cluster = Dse.cluster
      @@session = @@cluster.connect

      # Disabled for now until DSP-9410 is resolved.
      #self.remove_graph(@@session, 'users')
      #self.remove_graph(@@session, 'test')
      self.create_graph(@@session, 'users')
      self.create_graph(@@session, 'test')
      @@session.graph_options.graph_name = 'users'

      @@ccm_cluster.setup_graph_schema(<<-GRAPH, 'users')
      schema.propertyKey('name').Text().ifNotExists().create();
      schema.propertyKey('age').Int().ifNotExists().create();
      schema.propertyKey('lang').Text().ifNotExists().create();
      schema.propertyKey('weight').Float().ifNotExists().create();
      schema.vertexLabel('person').properties('name', 'age').ifNotExists().create();
      schema.vertexLabel('software').properties('name', 'lang').ifNotExists().create();
      schema.edgeLabel('created').properties('weight').connection('person', 'software').ifNotExists().create();
      schema.edgeLabel('created').connection('software', 'software').add();
      schema.edgeLabel('knows').properties('weight').connection('person', 'person').ifNotExists().create();
      GRAPH

      @@ccm_cluster.setup_graph_schema(<<-GRAPH, 'users')
      Vertex marko = graph.addVertex(label, 'person', 'name', 'marko', 'age', 29);
      Vertex vadas = graph.addVertex(label, 'person', 'name', 'vadas', 'age', 27);
      Vertex lop = graph.addVertex(label, 'software', 'name', 'lop', 'lang', 'java');
      Vertex josh = graph.addVertex(label, 'person', 'name', 'josh', 'age', 32);
      Vertex ripple = graph.addVertex(label, 'software', 'name', 'ripple', 'lang', 'java');
      Vertex peter = graph.addVertex(label, 'person', 'name', 'peter', 'age', 35);
      marko.addEdge('knows', vadas, 'weight', 0.5f);
      marko.addEdge('knows', josh, 'weight', 1.0f);
      marko.addEdge('created', lop, 'weight', 0.4f);
      josh.addEdge('created', ripple, 'weight', 1.0f);
      josh.addEdge('created', lop, 'weight', 0.4f);
      peter.addEdge('created', lop, 'weight', 0.2f);
      GRAPH

      # Adding a sleep here to allow for schema to propagate to all graph nodes
      sleep(2)
    end
  end

  def self.after_suite
    @@cluster.close unless CCM.dse_version < '5.0.0'
  end

  def self.create_graph(session, graph_name, rf = 3)
    replication_config = "{'class' : 'SimpleStrategy', 'replication_factor' : #{rf}}"
    session.execute_graph("system.graph('#{graph_name}').option('graph.replication_config').set(\"#{replication_config}\").ifNotExists().create()")

    begin
      session.execute_graph("schema.config().option('graph.schema_mode').set(com.datastax.bdp.graph.api.model.Schema.Mode.Production)", graph_name: graph_name)
    rescue Cassandra::Errors::InvalidError => e
      # Catch DSP-9199. Continuing for now.
      raise e unless e.message.include?('Shared data commit failed due to concurrent modification')
    end
  end

  def self.remove_graph(session, graph)
    begin
      session.execute_graph('g', graph_name: graph)
    rescue Cassandra::Errors::NoHostsAvailable => e
      e.errors.each do |(host, error)|
        raise e unless (error.is_a?(Cassandra::Errors::ServerError) && error.message.include?("Graph '#{graph}' does not exist"))
      end
      return
    end

    begin
      session.execute_graph("system.graph('#{graph}').drop()")
    rescue Cassandra::Errors::InvalidError => e
      # Catch DSP-9379. Continuing for now.
      raise e unless e.message.include?('Cannot drop graph while state is Dropping')
    end
  end

  def self.reset_schema(session, graph)
    # These won't work reliably until DSP-9405 is resolved.
    session.execute_graph('g.V().drop().iterate()', graph_name: graph)
    session.execute_graph('schema.clear()', graph_name: graph)
  end

  # Test for basic graph system queries
  #
  # test_session_graph_is_initially_nil tests that by default, no graph_name is specified and any graph query that uses
  # a specific graph fails. It also verifies that system graph queries still work, as these don't rely on any specific
  # graph.
  #
  # @expected_errors [Cassandra::Errors::InvalidError] When a graph query is executed
  #
  # @since 1.0.0
  # @jira_ticket RUBY-190
  # @expected_result graph-specific queries should fail, while system queries should succeed.
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_session_graph_is_initially_nil
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    session = @@cluster.connect

    assert_raises(Cassandra::Errors::InvalidError) do
      session.execute_graph('g.V()')
    end

    # Can still make system queries
    refute_nil session.execute_graph('system')
  end

  # Test for invalid graph queries
  #
  # test_raise_error_on_invalid_graph tests that an error is raised when a query is made against a graph that does not
  # exist.
  #
  # @expected_errors [Cassandra::Errors::NoHostsAvailable] When a graph query is executed against a non-existent graph
  #
  # @since 1.0.0
  # @jira_ticket RUBY-190
  # @expected_result the graph query should fail due to the non-existent graph
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_raise_error_on_invalid_graph
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    session = @@cluster.connect(graph_name: 'ffff')

    assert_raises(Cassandra::Errors::NoHostsAvailable) do
      session.execute_graph('g.V()')
    end
  end

  # Test for basic graph connection
  #
  # test_session_can_connect_to_existing_graph tests that a connection to an existing graph can be made, and a query
  # against that graph succeeds.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-190
  # @expected_result the graph connection should succeed and the query should execute
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_session_can_connect_to_existing_graph
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    session = @@cluster.connect(graph_name: 'users')

    assert_equal 'users', session.graph_name
    assert_equal 6, session.execute_graph('g.V().count()').first.value
    refute_nil session.execute_graph("g.V().has('name', 'marko')").first
  end

  # Test for switching graph connections
  #
  # test_can_switch_graphs_in_session tests that a connection to a graph can be specified at cluster connect, but also
  # can be changed later on in the session using graph options.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-200
  # @expected_result the graph connection should succeed to various graphs
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_switch_graphs_in_session
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    session = @@cluster.connect(graph_name: 'users')

    assert_equal 'users', session.graph_name
    assert_equal 6, session.execute_graph('g.V().count()').first.value

    session.graph_options.graph_name = 'test'
    assert_equal 'test', session.graph_name
    assert_equal 0, session.execute_graph('g.V().count()').first.value
  end

  # Test for setting a graph alias
  #
  # test_can_set_graph_alias tests that a graph alias can be specified in the graph options. It also verifies that this
  # alias is properly used during graph statement execution.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-200
  # @expected_result the graph alias should be usable during graph query execution
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_set_graph_alias
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    session = @@cluster.connect(graph_name: 'users', graph_alias: 'mygraph')

    assert_equal 'mygraph', session.graph_options.graph_alias
    assert_equal 6, session.execute_graph('mygraph.V()').size
  end

  # Test for setting graph consistencies
  #
  # test_can_set_graph_consistencies tests that a graph read and write consistencies can be set in the graph options. It
  # first sets a consistency of ALL in the cluster connect for both read and write. It then verifies that these
  # consistencies are properly set in the graph options. It then performs a read and write query to verify that the
  # consistencies are honored. It then stops one node, disabling the ability for consistency ALL to succeed, and
  # performs a read and write graph query and verifies a Cassandra::Errors::InvalidError is raised in each case.
  #
  # @expected_errors [Cassandra::Errors::InvalidError] When a graph query is executed without consistency ALL
  #
  # @since 1.0.0
  # @jira_ticket RUBY-200
  # @expected_result the graph consistency should be set and used during graph query execution
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_set_graph_consistencies
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    session = @@cluster.connect(graph_name: 'users', graph_read_consistency: :all, graph_write_consistency: :all)

    assert_equal :all, session.graph_options.graph_read_consistency
    assert_equal :all, session.graph_options.graph_write_consistency

    # First check that the consistencies work as expected
    assert_equal 6, session.execute_graph('g.V()').size
    session.execute_graph("graph.addVertex(label, 'person', 'name', 'yoda', 'age', 100);")
    assert_equal 7, session.execute_graph('g.V()').size
    session.execute_graph("g.V().has('person', 'name', 'yoda').drop()")
    assert_equal 6, session.execute_graph('g.V()').size

    @@ccm_cluster.stop_node('node1')

    # Read consistency failure
    assert_raises(Cassandra::Errors::InvalidError) do
      session.execute_graph('g.V()')
    end

    # Write consistency failure
    assert_raises(Cassandra::Errors::InvalidError) do
      session.execute_graph("graph.addVertex(label, 'person', 'name', 'yoda', 'age', 100);", timeout: 5)
    end

    @@ccm_cluster.start_node('node1')
  end

  # Test for using graph options in session and queries
  #
  # test_can_use_graph_options tests that graph options can be used in both sessions and queries. It first creates a
  # simple Dse::Graph::Options with graph_name and graph_alias parameters set. It then verifies that these settings are
  # honored when the graph options is used at session creation by executing a simple query. Finally it verifies that the
  # same graph options can be used at query execution.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-204
  # @expected_result graph options should be usable at session creation and query execution
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_use_graph_options
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    graph_options = Dse::Graph::Options.new(graph_name: 'users', graph_alias: 'mygraph')
    assert_equal 'users', graph_options.graph_name
    assert_equal 'mygraph', graph_options.graph_alias

    session = @@cluster.connect(graph_options: graph_options)

    assert_equal 'users', session.graph_name
    assert_equal 'mygraph', session.graph_options.graph_alias
    vertices = session.execute_graph('mygraph.V()')
    refute_nil vertices

    session.close
    session = @@cluster.connect
    second_vertices = session.execute_graph('mygraph.V()', graph_options: graph_options)
    refute_nil second_vertices
    assert_equal vertices, second_vertices
  end

  # Test for creating a new graph
  #
  # test_can_create_a_new_graph tests that a new graph can be created. It first creates a simple graph in DSE Graph,
  # and verifies that it is created by executing a simple query. It then removes the graph from DSE Graph.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-205
  # @expected_result a graph should be created and removed
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_create_a_new_graph
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    session = @@cluster.connect

    GraphTest.create_graph(session, 'new_graph')
    refute_nil session.execute_graph('g.V()', graph_name: 'new_graph')
    GraphTest.remove_graph(session, 'new_graph')
  end

  # Test for using graph query parameters
  #
  # test_can_use_graph_parameters tests that query parameters can be used during a graph query execution. It specifies
  # a parameterized graph query and then specifies the parameter as an argument, verifying that the query successfully
  # retrieves the expected values.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-193
  # @expected_result graph queries with arguments should execute successfully
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_use_graph_parameters
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    assert_equal 1, @@session.execute_graph('g.V().limit(my_limit)', arguments: {my_limit: 1}).size

    params = ['string', 1234, 3.14, true, false, nil]
    params.each do |param|
      assert_equal param, @@session.execute_graph('x', arguments: {x: param}).first.value
    end
  end

  # Test for tracing graph queries
  #
  # test_can_use_graph_trace tests that graph queries can be traced. It first executes a simple query and verifies that
  # no trace data is returned. It then executes the same query and verifies that trace data is returned.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-193
  # @expected_result graph queries should return trace data
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_use_graph_trace
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    assert_nil @@session.execute_graph('g.V()').execution_info.trace
    refute_nil @@session.execute_graph('g.V()', trace: true).execution_info.trace
  end

  # Test for using graph simple statements
  #
  # test_can_use_graph_statements tests that graph statements can be pre-created and used multiple times in execution.
  # It first creates a Dse::Graph::Statement with only a query specified, verifying its execution passes. It then
  # creates another Dse::Graph::Statement with both a query and parameters, verifying its execution. It finally creates
  # a Dse::Graph::Statement with a query and options, verifying its execution.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-196
  # @expected_result graph statements should be created and used
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_use_graph_statements
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    graph_query = 'g.V()'
    graph_statement = Dse::Graph::Statement.new(graph_query)
    assert_nil graph_statement.parameters
    assert_nil graph_statement.options
    assert_equal 6, @@session.execute_graph(graph_statement).size

    graph_query = 'g.V().limit(my_limit)'
    graph_statement = Dse::Graph::Statement.new(graph_query, parameters = {my_limit: 1})
    assert_equal graph_statement.parameters, {my_limit: 1}
    assert_nil graph_statement.options
    assert_equal 1, @@session.execute_graph(graph_statement).size

    graph_query = 'mygraph.V()'
    graph_options = Dse::Graph::Options.new({graph_name: 'users', graph_alias: 'mygraph'})
    graph_statement = Dse::Graph::Statement.new(graph_query, nil, options = graph_options)
    assert_nil graph_statement.parameters
    refute_nil graph_statement.options
    assert_equal 6, @@session.execute_graph(graph_statement).size
  end

end
