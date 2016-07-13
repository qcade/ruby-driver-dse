# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

require File.dirname(__FILE__) + '/integration_test_case.rb'
require 'set'

class SparkTest < IntegrationTestCase
  def self.before_suite
    if CCM.dse_version < '5.0.0'
      puts 'DSE > 5.0 required for graph and spark tests, skipping setup.'
    else
      @@ccm_cluster = CCM.setup_spark_cluster(1, 2)

      @@cluster = Dse.cluster
      @@session = @@cluster.connect
      create_graph(@@session, 'spark_test', 2)
      @@cluster.graph_options.graph_name = 'spark_test'

      @@ccm_cluster.setup_graph_schema(<<-GRAPH, 'spark_test')
      schema.propertyKey('name').Text().ifNotExists().create();
      schema.propertyKey('age').Int().ifNotExists().create();
      schema.propertyKey('lang').Text().ifNotExists().create();
      schema.propertyKey('weight').Float().ifNotExists().create();
      schema.vertexLabel('person').properties('name', 'age').ifNotExists().create();
      schema.vertexLabel('software').properties('name', 'lang').ifNotExists().create();
      GRAPH

      @@ccm_cluster.setup_graph_schema(<<-GRAPH, 'spark_test')
      Vertex marko = graph.addVertex(label, 'person', 'name', 'marko', 'age', 29);
      Vertex vadas = graph.addVertex(label, 'person', 'name', 'vadas', 'age', 27);
      Vertex lop = graph.addVertex(label, 'software', 'name', 'lop', 'lang', 'java');
      Vertex josh = graph.addVertex(label, 'person', 'name', 'josh', 'age', 32);
      Vertex ripple = graph.addVertex(label, 'software', 'name', 'ripple', 'lang', 'java');
      Vertex peter = graph.addVertex(label, 'person', 'name', 'peter', 'age', 35);
      GRAPH

      # Adding a sleep here to allow for schema to propagate to all graph nodes
      sleep(5)
    end
  end

  def self.create_graph(session, graph_name, rf = 3)
    replication_config = "{'class' : 'SimpleStrategy', 'replication_factor' : #{rf}}"
    session.execute_graph("system.graph('#{graph_name}').option('graph.replication_config').set(\"#{replication_config}\").ifNotExists().create()")
    session.execute_graph("schema.config().option('graph.schema_mode').set(com.datastax.bdp.graph.api.model.Schema.Mode.Production)", graph_name: graph_name)
    session.execute_graph("schema.config().option('graph.allow_scan').set('true')", graph_name: graph_name)
  end

  def self.after_suite
    @@cluster.close unless CCM.dse_version < '5.0.0'
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
  # @test_assumptions Graph and spark-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_analytics_runs_on_master
    skip('Graph and spark is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    # Run a query three times and verify that we always run against the same node.
    hosts = Set.new
    3.times do
      results = @@session.execute_graph('g.V().count()', graph_source: 'a')
      assert_equal 6, results.first.value
      hosts << results.execution_info.hosts.last.ip
    end

    assert_equal 1, hosts.size
  end

  # Test for retrieving vertex metadata using analytical queries
  #
  # test_analytics_return_vertex_properties tests that graph vertices can be retrieved, as well as their corresponding
  # metadata using analytical queries. It relies on pre-existing schema and vertex data. It retrieves one vertex and
  # verifies that all corresponding metadata is correct.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-195
  # @expected_result vertex should be retrieved and its metadata should be complete
  #
  # @test_assumptions Graph and spark-enabled Dse cluster.
  # @test_category dse:graph
  #
  def test_analytics_return_vertex_properties
    skip('Graph and spark is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    vertex = @@session.execute_graph("g.V().hasLabel('person').has('name', 'marko')",
                                     graph_source: 'a').first

    props = ['name', 'age']
    prop_values = ['marko', 29]

    id = vertex.id
    refute_nil id['~label']
    refute_nil id['member_id']
    refute_nil id['community_id']

    vertex.properties.each_pair do |property_name, property_values|
      assert props.include?(property_name), "expected #{props}, have #{property_name}"

      property_values.each do |property_value|
        assert prop_values.include?(property_value.value), "expected #{prop_values}, have #{property_value.value}"

        refute_nil property_value.id
        assert_empty property_value.properties
      end
    end
  end
end
