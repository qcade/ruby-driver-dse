@spark
@dse-version-specific @dse-version-5.0
Feature: Graph Analytics

  DSE Graph can be used in conjunction with Apache Spark embedded in DSE to perform Online Analytical Processing (OLAP)
  queries on graph datasets. OLAP can be enabled for graph queries by simply setting the `graph_source` graph option
  to 'a'. Graph OLAP queries will always be routed to the Spark master.

  Background:
    Given a running dse cluster with graph and spark enabled
    And an existing graph called "user_connections_spark" with schema:
      """gremlin
      schema.propertyKey('name').Text().ifNotExists().create();
      schema.propertyKey('age').Int().ifNotExists().create();
      schema.propertyKey('lang').Text().ifNotExists().create();
      schema.propertyKey('weight').Float().ifNotExists().create();
      schema.vertexLabel('person').properties('name', 'age').ifNotExists().create();
      schema.vertexLabel('software').properties('name', 'lang').ifNotExists().create();

      Vertex marko = graph.addVertex(label, 'person', 'name', 'marko', 'age', 29);
      Vertex vadas = graph.addVertex(label, 'person', 'name', 'vadas', 'age', 27);
      Vertex lop = graph.addVertex(label, 'software', 'name', 'lop', 'lang', 'java');
      Vertex josh = graph.addVertex(label, 'person', 'name', 'josh', 'age', 32);
      Vertex ripple = graph.addVertex(label, 'software', 'name', 'ripple', 'lang', 'java');
      Vertex peter = graph.addVertex(label, 'person', 'name', 'peter', 'age', 35);
      """

  Scenario: Running an OLAP graph query
    Given the following example:
      """ruby
      require 'dse'

      cluster = Dse.cluster(graph_name: 'user_connections_spark')
      session = cluster.connect

      results = session.execute_graph('g.V().count()', graph_source: 'a')
      puts "Result: #{results.first.value}"
      puts "The spark master was: #{results.execution_info.hosts.last.ip}"
      """
    When it is executed
    Then its output should contain:
      """
      Result: 6
      The spark master was: 127.0.0.1
      """

