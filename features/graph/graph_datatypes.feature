@graph
@dse-version-specific @dse-version-5.0
Feature: Graph Datatypes

  DSE Graph [supports a variety of datatypes](https://docs.datastax.com/en/latest-dse/datastax_enterprise/graph/reference/refDSEGraphDataTypes.html).
  Ruby driver for DSE transparently maps each of those datatypes to a specific Ruby type.

  Background:
    Given a running dse cluster with graph enabled
    And an existing graph called "datatypes" with schema:
      """gremlin
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
      """

  Scenario: Using strings
    Given the following example:
      """ruby
      require 'dse'

      cluster = Dse.cluster(graph_name: 'datatypes')
      session = cluster.connect

      blob = 'YmxvYg=='
      text = 'text'
      session.execute_graph("graph.addVertex(label, 'datatypes', 'blob', blob_value);
                             graph.addVertex(label, 'datatypes', 'text', text_value)",
                             arguments: {blob_value: blob,
                                         text_value: text
                             }
      )

      vertex = session.execute_graph("g.V().hasLabel('datatypes').has('blob')").first
      puts "Blob: #{vertex.properties['blob'].first.value}"
      vertex = session.execute_graph("g.V().hasLabel('datatypes').has('text')").first
      puts "Text: #{vertex.properties['text'].first.value}"
      """
    When it is executed
    Then its output should contain:
      """
      Blob: YmxvYg==
      Text: text
      """

  Scenario: Using numbers
    Given the following example:
      """ruby
      require 'dse'

      cluster = Dse.cluster(graph_name: 'datatypes')
      session = cluster.connect

      bigint  = 765438000
      decimal = ::BigDecimal.new('1313123123.234234234234234234123')
      double  = 3.141592653589793
      float   = 1.25
      int     = 4
      varint  = 67890656781923123918798273492834712837198237
      session.execute_graph("graph.addVertex(label, 'datatypes', 'bigint', bigint_value);
                             graph.addVertex(label, 'datatypes', 'decimal', decimal_value);
                             graph.addVertex(label, 'datatypes', 'double', double_value);
                             graph.addVertex(label, 'datatypes', 'float', float_value);
                             graph.addVertex(label, 'datatypes', 'int', int_value);
                             graph.addVertex(label, 'datatypes', 'varint', varint_value)",
                             arguments: {bigint_value:  bigint,
                                         decimal_value: decimal,
                                         double_value:  double,
                                         float_value:   float,
                                         int_value:     int,
                                         varint_value:  varint
                             }
      )

      vertex = session.execute_graph("g.V().hasLabel('datatypes').has('bigint')").first
      puts "Bigint: #{vertex.properties['bigint'].first.value}"
      vertex = session.execute_graph("g.V().hasLabel('datatypes').has('decimal')").first
      puts "Decimal: #{vertex.properties['decimal'].first.value}"
      vertex = session.execute_graph("g.V().hasLabel('datatypes').has('double')").first
      puts "Double: #{vertex.properties['double'].first.value}"
      vertex = session.execute_graph("g.V().hasLabel('datatypes').has('float')").first
      puts "Float: #{vertex.properties['float'].first.value}"
      vertex = session.execute_graph("g.V().hasLabel('datatypes').has('int')").first
      puts "Int: #{vertex.properties['int'].first.value}"
      vertex = session.execute_graph("g.V().hasLabel('datatypes').has('varint')").first
      puts "Varint: #{vertex.properties['varint'].first.value}"
      """
    When it is executed
    Then its output should contain:
      """
      Bigint: 765438000
      Decimal: 1313123123.2342343
      Double: 3.141592653589793
      Float: 1.25
      Int: 4
      Varint: 67890656781923123918798273492834712837198237
      """

  Scenario: Using identifiers, booleans and ip addresses
    Given the following example:
      """ruby
      require 'dse'

      cluster = Dse.cluster(graph_name: 'datatypes')
      session = cluster.connect

      boolean   = true
      inet      = ::IPAddr.new('200.199.198.197')
      timestamp = ::Time.at(1358013521, 123000)
      uuid      = Cassandra::Uuid.new('00b69180-d0e1-11e2-8b8b-0800200c9a66')
      session.execute_graph("graph.addVertex(label, 'datatypes', 'boolean', boolean_value);
                             graph.addVertex(label, 'datatypes', 'inet', inet_value);
                             graph.addVertex(label, 'datatypes', 'timestamp', timestamp_value);
                             graph.addVertex(label, 'datatypes', 'uuid', uuid_value)",
                             arguments: {boolean_value:   boolean,
                                         inet_value:      inet,
                                         timestamp_value: timestamp,
                                         uuid_value:      uuid
                             }
      )

      vertex = session.execute_graph("g.V().hasLabel('datatypes').has('boolean')").first
      puts "Boolean: #{vertex.properties['boolean'].first.value}"
      vertex = session.execute_graph("g.V().hasLabel('datatypes').has('inet')").first
      puts "Inet: #{vertex.properties['inet'].first.value}"
      vertex = session.execute_graph("g.V().hasLabel('datatypes').has('timestamp')").first
      puts "Timestamp: #{vertex.properties['timestamp'].first.value}"
      vertex = session.execute_graph("g.V().hasLabel('datatypes').has('uuid')").first
      puts "Uuid: #{vertex.properties['uuid'].first.value}"
      """
    When it is executed
    Then its output should contain:
      """
      Boolean: true
      Inet: 200.199.198.197
      Timestamp: 2013-01-12T17:58:41Z
      Uuid: 00b69180-d0e1-11e2-8b8b-0800200c9a66
      """

  Scenario: Using Duration datatype
    Given the following example:
      """ruby
      require 'dse'

      duration = Dse::Graph::Duration.new(2, 3, 1, 4.528)
      puts "Days: #{duration.days}"
      puts "Hours: #{duration.hours}"
      puts "Minutes: #{duration.minutes}"
      puts "Seconds: #{duration.seconds}"
      puts "Duration as seconds: #{duration.as_seconds}"
      puts "String form: #{duration.to_s}"
      puts ""

      duration2 = Dse::Graph::Duration.parse('P2DT3H1M4.528S')
      puts "String form: #{duration2.to_s}"
      duration2.days -= 2
      duration2.hours += 48
      duration2.minutes = -1
      duration2.seconds += 120
      puts "Duration as seconds: #{duration.as_seconds}"
      puts "String form: #{duration2.to_s}"
      puts "Are duration values equivalent? #{duration == duration2}"
      puts ""

      cluster = Dse.cluster(graph_name: 'datatypes')
      session = cluster.connect

      session.execute_graph("graph.addVertex(label, 'datatypes', 'duration', duration_value)",
                             arguments: {duration_value: duration}
      )

      vertex = session.execute_graph("g.V().hasLabel('datatypes').has('duration')").first
      puts "Duration: #{vertex.properties['duration'].first.value}"
      """
    When it is executed
    Then its output should contain:
      """
      Days: 2
      Hours: 3
      Minutes: 1
      Seconds: 4.528
      Duration as seconds: 183664.528
      String form: P2DT3H1M4.528S

      String form: P2DT3H1M4.528S
      Duration as seconds: 183664.528
      String form: P0DT51H-1M124.528S
      Are duration values equivalent? true

      Duration: PT51H1M4.528S
      """

  Scenario: Using geospatial types
    Given the following example:
      """ruby
      require 'dse'

      cluster = Dse.cluster(graph_name: 'datatypes')
      session = cluster.connect

      point       = Dse::Geometry::Point.new(38.0, 21.0)
      line_string = Dse::Geometry::LineString.new('LINESTRING (30 10, 10 30, 40 40)')
      polygon     = Dse::Geometry::Polygon.new('POLYGON ((0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0, 0.0 0.0),
                                                         (1.0 1.0, 4.0 9.0, 9.0 1.0, 1.0 1.0))'
      )

      session.execute_graph("graph.addVertex(label, 'datatypes', 'point', point_value);
                             graph.addVertex(label, 'datatypes', 'linestring', line_string_value);
                             graph.addVertex(label, 'datatypes', 'polygon', polygon_value)",
                             arguments: {point_value:       point,
                                         line_string_value: line_string,
                                         polygon_value:     polygon
                             }
      )

      vertex = session.execute_graph("g.V().hasLabel('datatypes').has('point')").first
      puts "Point: #{vertex.properties['point'].first.value}"
      vertex = session.execute_graph("g.V().hasLabel('datatypes').has('linestring')").first
      puts "Line_string: #{vertex.properties['linestring'].first.value}"
      vertex = session.execute_graph("g.V().hasLabel('datatypes').has('polygon')").first
      puts "Polygon: #{vertex.properties['polygon'].first.value}"
      """
    When it is executed
    Then its output should contain:
      """
      Point: POINT (38 21)
      Line_string: LINESTRING (30 10, 10 30, 40 40)
      Polygon: POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (1 1, 4 9, 9 1, 1 1))
      """

