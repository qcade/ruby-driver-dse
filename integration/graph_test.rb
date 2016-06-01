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
require 'set'

class GraphTest < IntegrationTestCase
  def self.before_suite
    if CCM.dse_version < '5.0.0'
      puts "DSE > 5.0 required for graph tests, skipping setup."
    else
      @@ccm_cluster = CCM.setup_graph_cluster(1, 3)

      @@cluster = Dse.cluster(timeout: 32)
      @@session = @@cluster.connect

      self.remove_graph(@@session, 'users')
      self.remove_graph(@@session, 'test')
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

      @@ccm_cluster.setup_graph_schema(<<-GRAPH, 'users')
      schema.propertyKey('multi_key').Text().multiple().ifNotExists().create();
      schema.propertyKey('single_key').Text().single().ifNotExists().create();
      schema.vertexLabel('namings').properties('multi_key', 'single_key').ifNotExists().create();
      schema.propertyKey('country').Text().ifNotExists().create();
      schema.propertyKey('descent').Text().ifNotExists().create();
      schema.propertyKey('origin').Text().properties('country', 'descent').ifNotExists().create();
      schema.vertexLabel('master').properties('name', 'origin').ifNotExists().create();
      schema.propertyKey('multi_origin').Text().multiple().properties('country').ifNotExists().create();
      schema.vertexLabel('multi_master').properties('name', 'multi_origin').ifNotExists().create();
      GRAPH

      # Adding a sleep here to allow for schema to propagate to all graph nodes
      sleep(5)
    end
  end

  def self.after_suite
    @@cluster.close unless CCM.dse_version < '5.0.0'
  end

  def self.create_graph(session, graph_name, rf = 3)
    replication_config = "{'class' : 'SimpleStrategy', 'replication_factor' : #{rf}}"
    session.execute_graph("system.graph('#{graph_name}').option('graph.replication_config').set(\"#{replication_config}\").ifNotExists().create()", timeout: 182)
    session.execute_graph("schema.config().option('graph.schema_mode').set(com.datastax.bdp.graph.api.model.Schema.Mode.Production)", graph_name: graph_name)
    session.execute_graph("schema.config().option('graph.allow_scan').set('true')", graph_name: graph_name)
  end

  def self.remove_graph(session, graph)
    if session.execute_graph("system.graph('#{graph}').exists()", timeout: 182).first.value
      session.execute_graph("system.graph('#{graph}').drop()", timeout: 182)
    end
  end

  def self.reset_schema(session, graph)
    session.execute_graph("schema.config().option('graph.traversal_sources.g.evaluation_timeout').set('PT120S')", graph_name: graph_name)
    session.execute_graph('g.V().drop().iterate()', graph_name: graph)
    session.execute_graph('schema.clear()', graph_name: graph)
    session.execute_graph("schema.config().option('graph.traversal_sources.g.evaluation_timeout').set('PT30S')", graph_name: graph_name)
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
  # simple Dse::Graph::Options with graph_name and graph_language parameters set. It then verifies that these settings are
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

    graph_options = Dse::Graph::Options.new(graph_name: 'users', graph_language: 'gremlin-groovy')
    assert_equal 'users', graph_options.graph_name
    assert_equal 'gremlin-groovy', graph_options.graph_language

    session = @@cluster.connect(graph_options: graph_options)

    assert_equal 'users', session.graph_name
    assert_equal 'gremlin-groovy', session.graph_options.graph_language
    vertices = session.execute_graph('g.V()')
    refute_nil vertices

    session.close
    session = @@cluster.connect
    second_vertices = session.execute_graph('g.V()', graph_options: graph_options)
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

    graph_query = 'g.V()'
    graph_options = Dse::Graph::Options.new({graph_name: 'users', graph_language: 'gremlin-groovy'})
    graph_statement = Dse::Graph::Statement.new(graph_query, nil, options = graph_options)
    assert_nil graph_statement.parameters
    refute_nil graph_statement.options
    assert_equal 6, @@session.execute_graph(graph_statement).size
  end

  # Test for running analytics queries against the analytics master
  #
  # test_run_analytics_on_master tests that graph statements run against an analytics source
  # execute on a particular node rather than round-robin.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-195
  # @expected_result graph statements should execute consistently on the analytics master
  #
  def test_run_analytics_on_master
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'
    skip('Spark tests are not operable right now')

    # There's a good chance the analytics server isn't quite ready for us yet. So, run
    # repeatedly until we get a result.
    num_attempts = 0
    while num_attempts < 10
      begin
        num_attempts += 1

        @@session.execute_graph('g.V().count()', graph_source: 'a')
        break
      rescue => e
        puts "Analytics query attempt #{num_attempts} failed: #{e}"
        sleep 10
      end
    end

    # Run a query three times and verify that we always run against the same node.
    hosts = Set.new
    3.times do
      hosts << @@session.execute_graph('g.V().count()', graph_source: 'a').execution_info.hosts.last.ip
    end
    assert_equal 1, hosts.size
  end

  def validate_vertex(vertex, label, props, prop_values, meta_properties = nil)
    assert_equal label, vertex.label

    id = vertex.id
    refute_nil id['~label']
    refute_nil id['member_id']
    refute_nil id['community_id']

    vertex.properties.each_pair do |property_name, property_values|
      assert props.include?(property_name), "expected #{props}, have #{property_name}"

      property_values.each do |property_value|
        assert prop_values.include?(property_value.value), "expected #{prop_values}, have #{property_value.value}"

        property_id = property_value.id
        refute_nil property_id['local_id']
        refute_nil property_id['~type']
        assert_equal id, property_id['out_vertex']

        if meta_properties && meta_properties[0] == property_name
          assert_equal meta_properties[1], property_value.properties
        else
          assert_empty property_value.properties
        end
      end
    end
  end

  # Test for retrieving vertex metadata
  #
  # test_can_retrieve_simple_vertex_metadata tests that graph vertices can be retrieved, as well as their corresponding
  # metadata. It relies on pre-existing schema and vertex data from 6 vertices. It retrieves each vertex and verifies
  # that all corresponding metadata is correct.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-194
  # @expected_result vertices should be retrieved and their metadata should be complete
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_retrieve_simple_vertex_metadata
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    results = @@session.execute_graph('g.V()')
    assert_equal 6, results.size

    labels = ['software', 'software', 'person', 'person', 'person', 'person']
    property_names = [['name', 'lang'], ['name', 'lang'], ['name', 'age'], ['name', 'age'], ['name', 'age'],
                      ['name', 'age']]
    property_values = [['lop', 'java'], ['ripple', 'java'], ['peter', 35], ['marko', 29], ['vadas', 27],
                       ['josh', 32]]

    results.each_with_index do |v, i|
      validate_vertex(v, labels[i], property_names[i], property_values[i])
    end
  end

  # Test for retrieving multi-value vertex property metadata
  #
  # test_can_retrieve_multi_value_vertex_properties tests that multi-value vertex property metadata is present in
  # vertices that has them. It relies on pre-existing schema, which includes a label 'namings', and two vertex properties
  # 'multi_key' and 'single_key'. 'multi_key' supports multiple property values while 'single_key' does not. It first
  # tests that it's possible to insert a single value to a multi-value property, verifying its metadata. It then verifies
  # the metadata for multiple values in a multi-value property. It then performs the same with a single-value property,
  # verifying that if multiple values are entered, the last entered value is the one stored as the property value. It
  # finally verifies that by default, property values are of the single-value type.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-194
  # @expected_result multi-value vertex property metadata should be complete
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_retrieve_multi_value_vertex_properties
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    # Single value in multi-value property
    @@session.execute_graph("graph.addVertex(label, 'namings', 'multi_key', 'value')")
    vertex = @@session.execute_graph("g.V().has('namings', 'multi_key', 'value')").first
    assert_equal 1, vertex.properties['multi_key'].size
    assert_equal 'value', vertex.properties['multi_key'][0].value
    @@session.execute_graph("g.V().has('namings', 'multi_key', 'value').drop()")

    # Multiple values in multi-value property
    @@session.execute_graph("graph.addVertex(label, 'namings', 'multi_key', 'value0', 'multi_key', 'value1')")
    vertex = @@session.execute_graph("g.V().has('namings', 'multi_key', 'value0')").first
    assert_equal 2, vertex.properties['multi_key'].size
    assert_equal 'value0', vertex.properties['multi_key'][0].value
    assert_equal 'value1', vertex.properties['multi_key'][1].value
    @@session.execute_graph("g.V().has('namings', 'multi_key', 'value0').drop()")

    # Single value in single-value property
    @@session.execute_graph("graph.addVertex(label, 'namings', 'single_key', 'value')")
    vertex = @@session.execute_graph("g.V().has('namings', 'single_key', 'value')").first
    assert_equal 1, vertex.properties['single_key'].size
    assert_equal 'value', vertex.properties['single_key'][0].value
    @@session.execute_graph("g.V().has('namings', 'single_key', 'value').drop()")

    # Properties by default are single-value
    @@session.execute_graph("graph.addVertex(label, 'person', 'name', 'john', 'name', 'doe')")
    vertex = @@session.execute_graph("g.V().has('person', 'name', 'doe')").first
    assert_equal 1, vertex.properties['name'].size
    assert_equal 'doe', vertex.properties['name'][0].value
    @@session.execute_graph("g.V().has('person', 'name', 'doe').drop()")
  end

  # Test for retrieving vertex property properties metadata
  #
  # test_can_retrieve_vertex_property_meta_properties tests that vertex property properties metadata is present in
  # vertex properties that have them. It relies on pre-existing schema, which includes a label 'master', and a vertex
  # property 'origin', which has two properties of itself 'country' and 'descent'. It first adds a new vertex which
  # use these schema values. It then retrieves the vertex and verifies that the metadata is correct, including the
  # presence of the vertex property properties.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-194
  # @expected_result vertex property properties metadata should be complete
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_retrieve_vertex_property_meta_properties
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    @@session.execute_graph("yoda = graph.addVertex(label, 'master', 'name', 'Yoda');
                             yoda.property('origin', 'unknown', 'country', 'Galactic Republic', 'descent', 'Jedi')")

    vertex =  @@session.execute_graph("g.V().has('master', 'name', 'Yoda')").first
    meta_properties = ['origin', {"country" => "Galactic Republic", "descent" => "Jedi"}]
    validate_vertex(vertex, 'master', ['name', 'origin'], ['Yoda', 'unknown'], meta_properties)

    @@session.execute_graph("g.V().has('master', 'name', 'Yoda').drop()")
  end

  # Test for retrieving multi-value properties and property properties metadata
  #
  # test_can_retrieve_multi_value_vertex_properties_with_meta_properties tests that both multi-value vertex properties
  # and vertex property properties metadata is present in vertices that have both of them. It relies on pre-existing
  # schema, which includes a label 'multi_master', and a vertex property 'multi_origin', which is a multi-value
  # property has two properties of itself 'country' and 'descent'. It first adds a new vertex which
  # use these schema values. It then retrieves the vertex and verifies that the metadata is correct, including the
  # presence of both the multi-value property and the vertex property properties.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-194
  # @expected_result vertex property properties metadata should be complete
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_retrieve_multi_value_vertex_properties_with_meta_properties
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    @@session.execute_graph("yoda = graph.addVertex(label, 'multi_master', 'name', 'Yoda');
                             yoda.property('multi_origin', 'unknown0', 'country', 'Galactic Republic');
                             yoda.property('multi_origin', 'unknown1', 'country', 'Jedi Order')")

    vertex =  @@session.execute_graph("g.V().has('multi_master', 'name', 'Yoda')").first
    assert_equal 2, vertex.properties['multi_origin'].size

    property_one = vertex.properties['multi_origin'][0]
    assert_equal 'unknown0', property_one.value
    assert_equal property_one.properties, {'country' => 'Galactic Republic'}

    property_two = vertex.properties['multi_origin'][1]
    assert_equal 'unknown1', property_two.value
    assert_equal property_two.properties, {'country' => 'Jedi Order'}

    @@session.execute_graph("g.V().has('multi_master', 'name', 'Yoda').drop()")
  end

  def validate_edge(edge, label, in_v, out_v, properties = nil)
    assert_equal label, edge.label

    id = edge.id
    refute_nil id['out_vertex']
    refute_nil id['in_vertex']
    refute_nil id['local_id']
    refute_nil id['~type']

    assert_equal in_v[0], edge.in_v_label
    assert_equal in_v[1]['~label'], edge.in_v['~label']
    assert_equal in_v[1]['member_id'], edge.in_v['member_id']

    assert_equal out_v[0], edge.out_v_label
    assert_equal out_v[1]['~label'], edge.out_v['~label']
    assert_equal out_v[1]['member_id'], edge.out_v['member_id']

    if properties
      assert_equal properties, edge.properties
    else
      assert_empty edge.properties
    end
  end

  # Test for retrieving edge metadata
  #
  # test_can_retrieve_simple_edge_metadata tests that graph edges can be retrieved, as well as their corresponding
  # metadata. It relies on pre-existing schema and edge data from 6 vertices. It retrieves each edge and verifies
  # that all corresponding metadata is correct.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-194
  # @expected_result edges should be retrieved and their metadata should be complete
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_retrieve_simple_edge_metadata
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    labels = ['created', 'created', 'knows' , 'knows', 'created', 'created']
    in_v = [['software', {"~label"=>"software", "member_id"=>1}],
            ['software', {"~label"=>"software", "member_id"=>1}],
            ['person', {"~label"=>"person", "member_id"=>3}],
            ['person', {"~label"=>"person", "member_id"=>4}],
            ['software', {"~label"=>"software", "member_id"=>1}],
            ['software', {"~label"=>"software", "member_id"=>5}]
    ]
    out_v = [['person', {"~label"=>"person", "member_id"=>0}],
             ['person', {"~label"=>"person", "member_id"=>2}],
             ['person', {"~label"=>"person", "member_id"=>2}],
             ['person', {"~label"=>"person", "member_id"=>2}],
             ['person', {"~label"=>"person", "member_id"=>4}],
             ['person', {"~label"=>"person", "member_id"=>4}]
    ]
    properties = [{"weight"=>0.2}, {"weight"=>0.4}, {"weight"=>0.5}, {"weight"=>1.0}, {"weight"=>0.4}, {"weight"=>1.0}]

    results = @@session.execute_graph('g.E()')
    assert_equal 6, results.size

    results.each_with_index do |e, i|
      validate_edge(e, labels[i], in_v[i], out_v[i], properties[i])
    end
  end

  # Test for retrieving path metadata
  #
  # test_can_retrieve_path_metadata tests that graph paths can be retrieved, as well as their corresponding
  # metadata. It relies on pre-existing schema, vertex, and edge data. It performs a path query which yields two
  # possible routes. It then iterates through each of these routes and verifies that the path metadata is complete.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-194
  # @expected_result paths should be retrieved and their metadata should be complete
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_retrieve_path_metadata
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    results = @@session.execute_graph("g.V().hasLabel('person').has('name', 'marko').as('a')" +
                                  ".outE('knows').inV().as('c', 'd').outE('created').as('e', 'f', 'g').inV().path()")
    assert_equal 2, results.size

    first_path = results[0].as_path
    assert_instance_of(Dse::Graph::Path, first_path)
    assert_equal [["a"], [], ["c", "d"], ["e", "f", "g"], []], first_path.labels

    path_objects = first_path.objects
    assert_equal 5, path_objects.size

    assert_equal 'marko', path_objects[0].properties['name'].first.value
    assert_equal 'knows', path_objects[1].label
    assert_equal 'josh', path_objects[2].properties['name'].first.value
    assert_equal 'created', path_objects[3].label
    assert_equal 'lop', path_objects[4].properties['name'].first.value
    assert_equal 'java', path_objects[4].properties['lang'].first.value

    second_path = results[1].as_path
    assert_instance_of(Dse::Graph::Path, second_path)
    assert_equal [["a"], [], ["c", "d"], ["e", "f", "g"], []], second_path.labels

    path_objects = second_path.objects
    assert_equal 5, path_objects.size

    assert_equal 'marko', path_objects[0].properties['name'].first.value
    assert_equal 'knows', path_objects[1].label
    assert_equal 'josh', path_objects[2].properties['name'].first.value
    assert_equal 'created', path_objects[3].label
    assert_equal 'ripple', path_objects[4].properties['name'].first.value
    assert_equal 'java', path_objects[4].properties['lang'].first.value
  end

  # Test for casting result into graph objects
  #
  # test_raise_error_on_casting tests that generic graph Results cannot be casted to object types of Vertex, Edge or
  # Path if the underlying result is not of the appropriate type.
  #
  # @expected_errors [ArgumentError] When the Result object is casted into Vertex, Edge or Path.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-194
  # @expected_result an ArgumentError should be raised during casting.
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_raise_error_on_casting
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    result = @@session.execute_graph("g.V().count()").first

    assert_raises(ArgumentError) do
      result.as_vertex
    end

    assert_raises(ArgumentError) do
      result.as_edge
    end

    assert_raises(ArgumentError) do
      result.as_path
    end
  end

end
