@graph
@dse-version-specific @dse-version-5.0
Feature: Graph

  DSE 5.0 introduced Graph. `session#execute_graph` and `session#execute_graph_async` can be used to send Gremlin
  graph queries to DSE Graph. The results (`DSE::Graph::ResultSet`) returned to the driver can include vertices
  (`DSE::Graph::Vertex`), edges (`DSE::Graph::Edge`), or a generic `DSE::Graph::Result` containing paths or other
  arbitrary objects.

  Background:
    Given a running dse cluster with graph enabled
    And an existing graph called "user_connections" with schema:
      """gremlin
      schema.propertyKey('name').Text().ifNotExists().create();
      schema.propertyKey('age').Int().ifNotExists().create();
      schema.propertyKey('lang').Text().ifNotExists().create();
      schema.propertyKey('weight').Float().ifNotExists().create();
      schema.vertexLabel('person').properties('name', 'age').ifNotExists().create();
      schema.vertexLabel('software').properties('name', 'lang').ifNotExists().create();

      schema.edgeLabel('created').properties('weight').connection('person', 'software').ifNotExists().create();
      schema.edgeLabel('knows').properties('weight').connection('person', 'person').ifNotExists().create();

      schema.propertyKey('country').Text().ifNotExists().create();
      schema.propertyKey('origin').Text().multiple().properties('country').ifNotExists().create();
      schema.vertexLabel('master').properties('name', 'origin').ifNotExists().create();
      schema.vertexLabel('character').properties('name').ifNotExists().create();
      """

  Scenario: Using graph options
    Given the following example:
      """ruby
      require 'dse'

      graph_options = Dse::Graph::Options.new(graph_name: 'user_connections',
                                              graph_source: 'g',
                                              graph_language: 'gremlin-groovy',
                                              graph_read_consistency: :quorum,
                                              graph_write_consistency: :one,
                                              timeout: 1
      )

      cluster = Dse.cluster(graph_options: graph_options)
      puts "Graph name: #{cluster.graph_options.graph_name}"
      puts "Graph source: #{cluster.graph_options.graph_source}"
      puts "Graph language: #{cluster.graph_options.graph_language}"
      puts "Graph read consistency: #{cluster.graph_options.graph_read_consistency}"
      puts "Graph write consistency: #{cluster.graph_options.graph_write_consistency}"
      puts "Timeout: #{cluster.graph_options.timeout}"
      puts ""

      cluster.graph_options.delete('graph_name')
      puts "Graph name nil?: #{cluster.graph_options.graph_name.nil?}"

      cluster.graph_options.set('graph_name', 'vendors')
      puts "Graph name: #{cluster.graph_options.graph_name}"

      cluster.graph_options.clear
      puts "Graph name nil?: #{cluster.graph_options.graph_name.nil?}"
      """
    When it is executed
    Then its output should contain:
      """
      Graph name: user_connections
      Graph source: g
      Graph language: gremlin-groovy
      Graph read consistency: quorum
      Graph write consistency: one
      Timeout: 1

      Graph name nil?: true
      Graph name: vendors
      Graph name nil?: true
      """

  Scenario: Inspecting vertices
    Given the following example:
      """ruby
      require 'dse'

      cluster = Dse.cluster(graph_name: 'user_connections')
      session = cluster.connect

      session.execute_graph("yoda = graph.addVertex(label, 'master', 'name', 'Yoda');
                             yoda.property('origin', 'unknown', 'country', 'Galactic Republic');
                             yoda.property('origin', 'secret', 'country', 'Jedi Order')")

      vertex = session.execute_graph("g.V().has('master', 'name', 'Yoda')").first
      puts "The result is a: #{vertex.class}"
      puts ""

      puts "Vertex has id?: #{!vertex.id.nil?}"
      puts "Vertex label: #{vertex.id['~label']}"
      puts ""

      vertex.properties.each_pair do |property_name, property_values|
        puts "Vertex property name: #{property_name}"

        property_values.each do |property_value|
          puts "Property value has id?: #{!property_value.id.nil?}"
          puts "Property value: #{property_value.value}"
          puts "Property's properties: #{property_value.properties}"
        end
        puts ""
      end

      """
    When it is executed
    Then its output should contain:
      """
      The result is a: Dse::Graph::Vertex

      Vertex has id?: true
      Vertex label: master

      Vertex property name: origin
      Property value has id?: true
      Property value: unknown
      Property's properties: {"country"=>"Galactic Republic"}
      Property value has id?: true
      Property value: secret
      Property's properties: {"country"=>"Jedi Order"}

      Vertex property name: name
      Property value has id?: true
      Property value: Yoda
      Property's properties: {}
      """

  Scenario: Using graph statements
    Given the following example:
      """ruby
      require 'dse'

      cluster = Dse.cluster
      session = cluster.connect

      graph_query     = 'g.V().limit(my_limit)'
      graph_options   = Dse::Graph::Options.new(graph_name: 'user_connections', graph_language: 'gremlin-groovy')
      graph_statement = Dse::Graph::Statement.new(graph_query, parameters = {my_limit: 1}, options = graph_options)
      puts "Statement parameters: #{graph_statement.parameters}"
      puts "Statement has graph options? #{!graph_statement.options.nil?}"
      puts "Graph query result size: #{session.execute_graph(graph_statement).size}"
      """
    When it is executed
    Then its output should contain:
      """
      Statement parameters: {:my_limit=>1}
      Statement has graph options? true
      Graph query result size: 1
      """

  Scenario: Inspecting edges
    Given the following example:
      """ruby
      require 'dse'

      cluster = Dse.cluster(graph_name: 'user_connections')
      session = cluster.connect

      session.execute_graph(<<-GREMLIN)
      Vertex marko = graph.addVertex(label, 'person', 'name', 'marko', 'age', 29);
      Vertex josh = graph.addVertex(label, 'person', 'name', 'josh', 'age', 32);
      Vertex lop = graph.addVertex(label, 'software', 'name', 'lop', 'lang', 'java');
      Vertex ripple = graph.addVertex(label, 'software', 'name', 'ripple', 'lang', 'java');
      marko.addEdge('knows', josh, 'weight', 1.0f);
      josh.addEdge('created', ripple, 'weight', 1.0f);
      josh.addEdge('created', lop, 'weight', 0.4f);
      GREMLIN

      edge = session.execute_graph("g.E()").first
      puts "The result is a: #{edge.class}"
      puts ""

      puts "Edge has id?: #{!edge.id.nil?}"
      puts "Edge label: #{edge.id['~type']}"
      puts "Edge incoming vertex label: #{edge.in_v_label}"
      puts "Edge outgoing vertex label: #{edge.out_v_label}"
      puts "Edge properties: #{edge.properties}"
      """
    When it is executed
    Then its output should contain:
      """
      The result is a: Dse::Graph::Edge

      Edge has id?: true
      Edge label: created
      Edge incoming vertex label: software
      Edge outgoing vertex label: person
      Edge properties: {"weight"=>0.4}
      """

  Scenario: Inspecting paths
    Given the following example:
      """ruby
      require 'dse'

      cluster = Dse.cluster(graph_name: 'user_connections')
      session = cluster.connect

      paths = session.execute_graph("g.V().hasLabel('person').has('name', 'marko').as('a')" \
                                  ".outE('knows').inV().as('c', 'd').outE('created').as('e', 'f', 'g').inV().path()")

      puts "Total paths found: #{paths.size}"
      puts ""

      first_path = paths[0].as_path
      puts "Path labels: #{first_path.labels}"

      path_objects = first_path.objects
      puts "Length of path: #{path_objects.size}"
      puts "Object 1 is a #{path_objects[0].class}, value: #{path_objects[0].properties['name'].first.value}"
      puts "Object 2 is a #{path_objects[1].class}, value: #{path_objects[1].label}"
      puts "Object 3 is a #{path_objects[2].class}, value: #{path_objects[2].properties['name'].first.value}"
      puts "Object 4 is a #{path_objects[3].class}. value: #{path_objects[3].label}"
      puts "Object 5 is a #{path_objects[4].class}, value: #{path_objects[4].properties['name'].first.value}"
      puts ""

      second_path = paths[1].as_path
      puts "Path labels: #{second_path.labels}"

      path_objects = second_path.objects
      puts "Length of path: #{path_objects.size}"
      puts "Object 1 is a #{path_objects[0].class}, value: #{path_objects[0].properties['name'].first.value}"
      puts "Object 2 is a #{path_objects[1].class}, value: #{path_objects[1].label}"
      puts "Object 3 is a #{path_objects[2].class}, value: #{path_objects[2].properties['name'].first.value}"
      puts "Object 4 is a #{path_objects[3].class}. value: #{path_objects[3].label}"
      puts "Object 5 is a #{path_objects[4].class}, value: #{path_objects[4].properties['name'].first.value}"
      """
    When it is executed
    Then its output should contain:
      """
      Total paths found: 2

      Path labels: [["a"], [], ["c", "d"], ["e", "f", "g"], []]
      Length of path: 5
      Object 1 is a Dse::Graph::Vertex, value: marko
      Object 2 is a Dse::Graph::Edge, value: knows
      Object 3 is a Dse::Graph::Vertex, value: josh
      Object 4 is a Dse::Graph::Edge. value: created
      Object 5 is a Dse::Graph::Vertex, value: lop

      Path labels: [["a"], [], ["c", "d"], ["e", "f", "g"], []]
      Length of path: 5
      Object 1 is a Dse::Graph::Vertex, value: marko
      Object 2 is a Dse::Graph::Edge, value: knows
      Object 3 is a Dse::Graph::Vertex, value: josh
      Object 4 is a Dse::Graph::Edge. value: created
      Object 5 is a Dse::Graph::Vertex, value: ripple
      """

  Scenario: Inspecting arbitrary results
    Given the following example:
      """ruby
      require 'dse'

      cluster = Dse.cluster(graph_name: 'user_connections')
      session = cluster.connect

      result = session.execute_graph('g.V().count()').first
      puts "The result is a: #{result.class}"
      puts "The value of the result is: #{result.value}"
      """
    When it is executed
    Then its output should contain:
      """
      The result is a: Dse::Graph::Result
      The value of the result is: 5
      """

  Scenario: Using graph query parameters
    Given the following example:
      """ruby
      require 'dse'

      cluster = Dse.cluster(graph_name: 'user_connections')
      session = cluster.connect

      # Simple parameter
      results = session.execute_graph('g.V().limit(my_limit)', arguments: {my_limit: 1})
      puts "Returned #{results.size} result(s)"
      puts ""

      # List as parameter
      characters = ['Mario', 'Luigi', 'Toad', 'Bowser', 'Peach', 'Wario', 'Waluigi']
      insert = "characters.each { character -> \n" +
               "    graph.addVertex(label, 'character', 'name', character);\n" +
               "}"

      session.execute_graph(insert, arguments: {characters: characters})

      results = session.execute_graph("g.V().hasLabel('character').values('name')")
      puts "There are #{results.size} characters total"
      results.map { |result| puts result.value }
      puts ""

      # Map as parameter
      input_map = { name: 'Yoda2', origin: 'unknown', origin_properties: ['Galactic Republic'] }
      session.execute_graph("yoda = graph.addVertex(label, 'master', 'name', input_map.name);
                             yoda.property('origin', input_map.origin, 'country', input_map.origin_properties[0])",
                             arguments: {input_map: input_map}
      )

      vertex = session.execute_graph("g.V().has('master', 'name', 'Yoda2')").first
      puts "Vertex label: #{vertex.id['~label']}"
      vertex.properties.each_pair do |property_name, property_values|
        puts "Vertex property name: #{property_name}"

        property_values.each do |property_value|
          puts "Property value: #{property_value.value}"
          puts "Property's properties: #{property_value.properties}"
        end
      end
      """
    When it is executed
    Then its output should contain:
      """
      Returned 1 result(s)

      There are 7 characters total
      Mario
      Luigi
      Toad
      Bowser
      Peach
      Wario
      Waluigi

      Vertex label: master
      Vertex property name: origin
      Property value: unknown
      Property's properties: {"country"=>"Galactic Republic"}
      Vertex property name: name
      Property value: Yoda2
      Property's properties: {}
      """

