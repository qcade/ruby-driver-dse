# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

require File.dirname(__FILE__) + '/integration_test_case.rb'
require File.dirname(__FILE__) + '/datatype_utils.rb'
require File.dirname(__FILE__) + '/ordered_loadbalancing_policy.rb'
require 'set'

include Dse::Geometry

class GraphTest < IntegrationTestCase
  def self.before_suite
    if CCM.dse_version < '5.0.0'
      puts 'DSE > 5.0 required for graph tests, skipping setup.'
    else
      @@ccm_cluster = CCM.setup_graph_cluster(1, 3)

      @@cluster = Dse.cluster
      @@session = @@cluster.connect

      remove_graph(@@session, 'users')
      remove_graph(@@session, 'test')
      create_graph(@@session, 'users')
      create_graph(@@session, 'test')

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

      @@ccm_cluster.setup_graph_schema(<<-GRAPH, 'users')
      schema.propertyKey('bigint').Bigint().ifNotExists().create();
      schema.propertyKey('blob').Blob().ifNotExists().create();
      schema.propertyKey('boolean').Boolean().ifNotExists().create();
      schema.propertyKey('decimal').Decimal().ifNotExists().create();
      schema.propertyKey('double').Double().ifNotExists().create();
      schema.propertyKey('duration').Duration().ifNotExists().create();
      schema.propertyKey('float').Float().ifNotExists().create();
      schema.propertyKey('inet').Inet().ifNotExists().create();
      schema.propertyKey('int').Int().ifNotExists().create();
      schema.propertyKey('text').Text().ifNotExists().create();
      schema.propertyKey('timestamp').Timestamp().ifNotExists().create();
      schema.propertyKey('uuid').Uuid().ifNotExists().create();
      schema.propertyKey('varint').Varint().ifNotExists().create();
      schema.propertyKey('smallint').Smallint().ifNotExists().create();
      schema.propertyKey('point').Point().ifNotExists().create();
      schema.propertyKey('linestring').Linestring().ifNotExists().create();
      schema.propertyKey('polygon').Polygon().ifNotExists().create();
      schema.vertexLabel('datatypes').properties('bigint', 'blob', 'boolean', 'decimal', 'double', 'duration',
                                                 'float', 'inet', 'int', 'text', 'timestamp', 'uuid', 'varint',
                                                 'smallint', 'point', 'linestring', 'polygon').ifNotExists().create();
      GRAPH

      @@ccm_cluster.setup_graph_schema(<<-GRAPH, 'users')
      schema.propertyKey('characterName').Text().create();
      schema.vertexLabel('character').properties('characterName').create();
      GRAPH
      
      # Adding a sleep here to allow for schema to propagate to all graph nodes
      sleep(5)
    end
  end

  def self.after_suite
    @@cluster.close unless CCM.dse_version < '5.0.0'
  end

  def setup
    unless CCM.dse_version < '5.0.0'
      @@cluster.graph_options.clear
      @@cluster.graph_options.graph_name = 'users'
    end
  end

  def self.create_graph(session, graph_name, rf = 3)
    replication_config = "{'class' : 'SimpleStrategy', 'replication_factor' : #{rf}}"
    session.execute_graph("system.graph('#{graph_name}').option('graph.replication_config').set(\"#{replication_config}\").ifNotExists().create()")
    session.execute_graph("schema.config().option('graph.schema_mode').set(com.datastax.bdp.graph.api.model.Schema.Mode.Production)", graph_name: graph_name)
    session.execute_graph("schema.config().option('graph.allow_scan').set('true')", graph_name: graph_name)
  end

  def self.remove_graph(session, graph)
    if session.execute_graph("system.graph('#{graph}').exists()").first.value
      session.execute_graph("system.graph('#{graph}').drop()")
    end
  end

  def self.reset_schema(session, graph_name)
    session.execute_graph("schema.config().option('graph.traversal_sources.g.evaluation_timeout').set('PT120S')", graph_name: graph_name)
    session.execute_graph('g.V().drop().iterate()', graph_name: graph_name)
    session.execute_graph('schema.clear()', graph_name: graph_name)
    session.execute_graph("schema.config().option('graph.traversal_sources.g.evaluation_timeout').set('PT30S')", graph_name: graph_name)
  end

  # Test for basic graph system queries
  #
  # test_cluster_graph_is_initially_nil tests that by default, no graph_name is specified and any graph query that uses
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
  def test_cluster_graph_is_initially_nil
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    @@cluster.graph_options.clear

    assert_raises(Cassandra::Errors::InvalidError) do
      @@session.execute_graph('g.V()')
    end

    # Can still make system queries
    refute_nil @@session.execute_graph('system')
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

    assert_raises(Cassandra::Errors::NoHostsAvailable) do
      @@session.execute_graph('g.V()', graph_name: 'ffff')
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

    # The cluster graph-name is set to 'users' in setup.
    assert_equal 6, @@session.execute_graph('g.V().count()').first.value
    refute_nil @@session.execute_graph("g.V().has('name', 'marko')").first
  end

  # Test for switching graph connections
  #
  # test_can_switch_graphs_in_cluster tests that a connection to a graph can be specified when creating the cluster
  # object, but also can be changed later on in the cluster using graph options.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-200
  # @expected_result the graph connection should succeed to various graphs
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_switch_graphs_in_cluster
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    # The graph-name is 'users', set in setup.
    assert(@@session.execute_graph('g.V().count()').first.value > 0)

    @@cluster.graph_options.graph_name = 'test'
    assert_equal 0, @@session.execute_graph('g.V().count()').first.value
  end

  # Test for setting graph consistencies
  #
  # test_can_set_graph_consistencies tests that a graph read and write consistencies can be set in the graph options. It
  # first sets a consistency of ALL on the cluster object for both read and write. It then verifies that these
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

    @@cluster.graph_options.merge!(
        Dse::Graph::Options.new(graph_name: 'users', graph_read_consistency: :all, graph_write_consistency: :all))

    assert_equal :all, @@cluster.graph_options.graph_read_consistency
    assert_equal :all, @@cluster.graph_options.graph_write_consistency

    # First check that the consistencies work as expected
    assert_equal 6, @@session.execute_graph('g.V()').size
    @@session.execute_graph("graph.addVertex(label, 'person', 'name', 'yoda', 'age', 100);")
    assert_equal 7, @@session.execute_graph('g.V()').size
    @@session.execute_graph("g.V().has('person', 'name', 'yoda').drop()")
    assert_equal 6, @@session.execute_graph('g.V()').size

    @@ccm_cluster.stop_node('node1')

    # Read consistency failure
    assert_raises(Cassandra::Errors::InvalidError) do
      @@session.execute_graph('g.V()')
    end

    # Write consistency failure
    assert_raises(Cassandra::Errors::InvalidError) do
      @@session.execute_graph("graph.addVertex(label, 'person', 'name', 'yoda', 'age', 100);", timeout: 5)
    end

    @@ccm_cluster.start_node('node1')
  end

  # Test for setting server side timeouts
  #
  # test_can_send_graph_timeout_to_server tests that the driver is able to send a server-side timeout to DSE Graph,
  # to be used as the timeout for graph queries. It performs a simple graph query with a request_timeout, which will
  # be sent to DSE Graph. Since this timeout is very small, it will always raise a server-side timeout. We verify that
  # this timeout is raised. It performs this same test once more using the graph options. Finally, it performs the
  # same test one last time, but blocking all the nodes to verify that a TimeoutError is triggered.
  #
  # @expected_errors [Cassandra::Errors::InvalidError] When the graph query is executed using a small request_timeout
  # @expected_errors [Cassandra::Errors::TimeoutError] When the graph query is executed with all nodes down
  #
  # @since 1.0.0
  # @jira_ticket RUBY-210
  # @expected_result the graph server-side timeout should be set and used during graph query execution
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_send_graph_timeout_to_server
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    assert_raises(Cassandra::Errors::InvalidError) do
      @@session.execute_graph("g.V()", timeout: 0.01)
    end

    @@cluster.graph_options.timeout = 0.01
    assert_raises(Cassandra::Errors::InvalidError) do
      @@session.execute_graph("g.V()")
    end
    @@cluster.graph_options.clear

    @@ccm_cluster.block_nodes
    assert_raises(Cassandra::Errors::TimeoutError) do
      @@session.execute_graph("g.V()", timeout: 0.01)
    end
  ensure
    @@ccm_cluster.unblock_nodes
  end

  # Test for retrying idempotent statements on timeout
  #
  # test_graph_statement_idempotency_on_timeout tests that idempotent graph statements are retried automatically on the
  # next host. It first blocks the first two hosts such that they are unreachable. It then attempts a simple g.V()
  # graph statement and verifies that a Cassandra::Errors::TimeoutError is raised, and the next host is not tried. It
  # then executes the same statement with idempotent explicitly set to false, verifying the same
  # Cassandra::Errors::TimeoutError being raised. Finally executes the statement once more with idempotent: true and
  # verifies that the statement executes successfully on another host.
  #
  # @expected_errors [Cassandra::Errors::TimeoutError] When a host is unavailable on a non-idempotent query
  #
  # @since 1.0.0
  # @jira_ticket RUBY-225
  # @expected_result Idempotent queries should be retried on the next host automatically
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_graph_statement_idempotency_on_timeout
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    policy  = OrderedPolicy.new(Cassandra::LoadBalancing::Policies::RoundRobin.new)
    cluster = Dse.cluster(graph_name: 'users', load_balancing_policy: policy)
    session = cluster.connect


    @@ccm_cluster.block_node('node1')
    @@ccm_cluster.block_node('node2')

    assert_raises(Cassandra::Errors::TimeoutError) do
      session.execute_graph('g.V()', timeout: 5)
    end

    assert_raises(Cassandra::Errors::TimeoutError) do
      session.execute_graph('g.V()', timeout: 5, idempotent: false)
    end

    Retry.with_attempts(5, Cassandra::Errors::InvalidError, Cassandra::Errors::NoHostsAvailable) do
      info = session.execute_graph('g.V()', timeout: 5, idempotent: true).execution_info
      assert_equal 3, info.hosts.size
      assert_equal '127.0.0.1', info.hosts[0].ip.to_s
      assert_equal '127.0.0.2', info.hosts[1].ip.to_s
    end
  ensure
    if CCM.dse_version >= '5.0.0'
      @@ccm_cluster.unblock_nodes
      cluster.close
    end
  end

  # Test for using graph options in session and queries
  #
  # test_can_use_graph_options tests that graph options can be used in both the cluster and queries. It first creates a
  # simple Dse::Graph::Options with graph_name and graph_language parameters set. It then verifies that these settings are
  # honored when the graph options are merged into the cluster's graph options by executing a simple query. It then
  # verifies that we are able to clear graph options. Finally it clears out all existing cluster graph options and
  # verifies that the same graph options can be used at query execution.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-204
  # @expected_result graph options should be usable in cluster object and query execution
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_use_graph_options
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    graph_options = Dse::Graph::Options.new(graph_name: 'users', graph_language: 'gremlin-groovy')
    assert_equal 'users', graph_options.graph_name
    assert_equal 'gremlin-groovy', graph_options.graph_language

    @@cluster.graph_options.merge!(graph_options)

    assert_equal 'users', @@cluster.graph_options.graph_name
    assert_equal 'gremlin-groovy', @@cluster.graph_options.graph_language
    vertices = @@session.execute_graph('g.V()')
    refute_nil vertices

    # Options can be reset
    @@cluster.graph_options.delete('graph_name')
    assert_nil @@cluster.graph_options.graph_name

    # Clear the graph options to be sure that the graph options specified in the query has an effect.
    @@cluster.graph_options.clear
    second_vertices = @@session.execute_graph('g.V()', graph_options: graph_options)
    refute_nil second_vertices
    assert_equal vertices, second_vertices
  end

  # Test for using graph transaction configuration options
  #
  # test_can_use_graph_expert_options tests that graph transaction configuration options, or 'expert options' can be
  # used in graph options as a custom payload. It first checks that the initial value of 'graph-name' option is 'users'.
  # It then sets 'graph-name' to be 'test' and verifies that DSE graph returns the results for the proper graph. It then
  # sets the option to nil and verifies that the nil option is ignored. Finally, it verifies that the transaction
  # configuration option can be cleared.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-222
  # @expected_result graph transaction configuration options should be set and used in query execution
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_use_graph_expert_options
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    # Initial value
    assert_equal('users', @@cluster.graph_options.as_payload['graph-name'])

    # Set graph-name
    @@cluster.graph_options.set('graph-name', 'test')
    assert_equal('test', @@cluster.graph_options.as_payload['graph-name'])
    assert_equal(0, @@session.execute_graph('g.V().count()').first.value)

    # Nil options are ignored
    @@cluster.graph_options.set('graph-name', nil)
    assert_equal('test', @@cluster.graph_options.as_payload['graph-name'])
    assert_equal(0, @@session.execute_graph('g.V().count()').first.value)

    # Reset options
    @@cluster.graph_options.delete('graph-name')
    assert_nil @@cluster.graph_options.as_payload['graph-name']
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

    @@cluster.graph_options.clear

    GraphTest.create_graph(@@session, 'new_graph')
    refute_nil @@session.execute_graph('g.V()', graph_name: 'new_graph')
    GraphTest.remove_graph(@@session, 'new_graph')
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
    graph_options = Dse::Graph::Options.new(graph_name: 'users', graph_language: 'gremlin-groovy')
    graph_statement = Dse::Graph::Statement.new(graph_query, nil, options = graph_options)
    assert_nil graph_statement.parameters
    refute_nil graph_statement.options
    assert_equal 6, @@session.execute_graph(graph_statement).size
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

    vertex = @@session.execute_graph("g.V().has('master', 'name', 'Yoda')").first
    meta_properties = ['origin', {'country' => 'Galactic Republic', 'descent' => 'Jedi'}]
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

    vertex = @@session.execute_graph("g.V().has('multi_master', 'name', 'Yoda')").first
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

    labels = %w(created created knows knows created created)
    in_v = [['software', {'~label' => 'software', 'member_id' => 1}],
            ['software', {'~label' => 'software', 'member_id' => 1}],
            ['person', {'~label' => 'person', 'member_id' => 3}],
            ['person', {'~label' => 'person', 'member_id' => 4}],
            ['software', {'~label' => 'software', 'member_id' => 1}],
            ['software', {'~label' => 'software', 'member_id' => 5}]
    ]
    out_v = [['person', {'~label' => 'person', 'member_id' => 0}],
             ['person', {'~label' => 'person', 'member_id' => 2}],
             ['person', {'~label' => 'person', 'member_id' => 2}],
             ['person', {'~label' => 'person', 'member_id' => 2}],
             ['person', {'~label' => 'person', 'member_id' => 4}],
             ['person', {'~label' => 'person', 'member_id' => 4}]
    ]
    properties = [{'weight' => 0.2}, {'weight' => 0.4}, {'weight' => 0.5}, {'weight' => 1.0}, {'weight' => 0.4}, {'weight' => 1.0}]

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

    results = @@session.execute_graph("g.V().hasLabel('person').has('name', 'marko').as('a')" \
                                  ".outE('knows').inV().as('c', 'd').outE('created').as('e', 'f', 'g').inV().path()")
    assert_equal 2, results.size

    first_path = results[0].as_path
    assert_instance_of(Dse::Graph::Path, first_path)
    assert_equal [['a'], [], ['c', 'd'], ['e', 'f', 'g'], []], first_path.labels

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
    assert_equal [['a'], [], ['c', 'd'], ['e', 'f', 'g'], []], second_path.labels

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

    result = @@session.execute_graph('g.V().count()').first

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

  # Test for Duration Graph datatype
  #
  # test_can_use_duration_datatype tests that the Duration graph datatype can be used. It first creates a simple
  # Duration object and verifies its attributes. It then creates this same object, but using the duration string form.
  # It finally tests some invalid and valid Duration constructors.
  #
  # @expected_errors [ArgumentError] When invalid constructors are used
  #
  # @since 1.0.0
  # @jira_ticket RUBY-230
  # @expected_result duration datatype should be created
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_use_duration_datatype
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    duration = Dse::Graph::Duration.new(2, 3, 1, 4.528)
    assert_equal 2, duration.days
    assert_equal 3, duration.hours
    assert_equal 1, duration.minutes
    assert_equal 4.528, duration.seconds
    assert_equal 'P2DT3H1M4.528S', duration.to_s

    duration2 = Dse::Graph::Duration.parse('P2DT3H1M4.528S')
    assert_equal duration, duration2

    duration2.days -= 2
    duration2.hours += 48
    duration2.minutes = -1
    duration2.seconds += 120
    assert_equal 'P0DT51H-1M124.528S', duration2.to_s
    assert_equal duration, duration2

    # Invalid duration string form
    assert_raises(ArgumentError) do
      Dse::Graph::Duration.parse('P2DTfff3H1M4.528S')
    end

    # Empty constructor is invalid
    assert_raises(ArgumentError) do
      Dse::Graph::Duration.new
    end

    # Nil args are parsed into 0
    duration3 = Dse::Graph::Duration.new(nil, nil, nil, nil)
    assert_equal 'P0DT0H0M0.0S', duration3.to_s
  end

  # Test for creating and retrieving vertices with all graph datatypes
  #
  # test_can_create_vertex_with_all_datatypes tests that vertices can be inserted which have vertex properties with
  # all the supported graph datatypes. It goes through each graph datatype, adding a vertex using each datatype as
  # a vertex property. It then retrieves this vertex and verifies that the vertex property data is retrieved as
  # expected.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-197
  # @expected_result Vertices should be created and retrieved with graph datatypes
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_create_vertex_with_all_datatypes
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    DatatypeUtils.graph_datatypes.each do |datatype|
      input_value = DatatypeUtils.get_sample(datatype)
      @@session.execute_graph("graph.addVertex(label, 'datatypes', '#{datatype}', input_value)",
                              arguments: { input_value: input_value})
      vertex = @@session.execute_graph("g.V().hasLabel('datatypes').has('#{datatype}')").first
      returned_value =  vertex.properties[datatype].first.value

      if input_value.is_a?(Numeric) || [true, false].include?(input_value) || datatype == 'blob' || datatype == 'text'
        if input_value.class == ::BigDecimal
          assert_equal input_value.to_f, returned_value
        else
          assert_equal input_value, returned_value
        end
      elsif datatype == 'duration'
        assert_equal input_value, Dse::Graph::Duration.parse(returned_value)
      elsif datatype == 'inet'
        assert_equal input_value, ::IPAddr.new(returned_value)
      elsif datatype == 'timestamp'
        assert_equal input_value.to_i, Time.parse(returned_value).to_i
      elsif datatype == 'uuid'
        assert_equal input_value, Cassandra::Uuid.new(returned_value)
      elsif datatype == 'point'
        assert_equal input_value, Dse::Geometry::Point.new(returned_value)
      elsif datatype == 'linestring'
        assert_equal input_value, Dse::Geometry::LineString.new(returned_value)
      elsif datatype == 'polygon'
        assert_equal input_value, Dse::Geometry::Polygon.new(returned_value)
      else
        flunk("Missing handling of '#{datatype}'")
      end

      @@session.execute_graph("g.V().hasLabel('datatypes').has('#{datatype}').drop()")
    end
  end

  # Test for using a list as a graph query parameter
  #
  # test_can_use_list_as_parameter tests lists can be used as a parameter when inserting vertices using a graph query.
  # It first creates a list of names to be used as the parameter. It then executes a graph statement that adds vertices
  # using this constructed list of names as the parameter. It then retrieves these vertices and verifies that the
  # parameters have been used properly.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-190
  # @expected_result Vertices should be created using a list as a parameter
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_use_list_as_parameter
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    characters = ['Mario', "Luigi", "Toad", "Bowser", "Peach", "Wario", "Waluigi"]
    insert = "characters.each { character -> \n" +
             "    graph.addVertex(label, 'character', 'characterName', character);\n" +
             "}"

    @@session.execute_graph(insert, arguments: {characters: characters})
    results = @@session.execute_graph("g.V().hasLabel('character').values('characterName')")
    assert_equal 7, results.size
    retrieved_characters = results.map { |result| result.value }

    assert_equal characters, retrieved_characters
    @@session.execute_graph("g.V().hasLabel('character').drop()")
  end

  # Test for using a map as a graph query parameter
  #
  # test_can_use_map_as_parameter tests maps can be used as a parameter when inserting vertices using a graph query.
  # It first creates a map of arguments to be used as the parameter. It then executes a graph statement that adds a
  # vertex using this constructed map of arguments as the parameter. It then retrieves the vertex and verifies that the
  # parameters have been used properly.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-190
  # @expected_result Vertices should be created using a map as a parameter
  #
  # @test_assumptions Graph-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_can_use_map_as_parameter
    skip('Graph is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    origin_properties = ['Galactic Republic', 'Jedi']
    input_map = { name: 'Yoda', origin: 'unknown', origin_properties: origin_properties }

    @@session.execute_graph("yoda = graph.addVertex(label, 'master', 'name', input_map.name);
                             yoda.property('origin', input_map.origin, 'country', input_map.origin_properties[0],
                             'descent', input_map.origin_properties[1])", arguments: {input_map: input_map})

    vertex = @@session.execute_graph("g.V().has('master', 'name', 'Yoda')").first
    meta_properties = ['origin', {'country' => 'Galactic Republic', 'descent' => 'Jedi'}]
    validate_vertex(vertex, 'master', ['name', 'origin'], ['Yoda', 'unknown'], meta_properties)

    @@session.execute_graph("g.V().has('master', 'name', 'Yoda').drop()")
  end

end
