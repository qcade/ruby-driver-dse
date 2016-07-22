@dse-version-specific @dse-version-5.0
Feature: Geospatial Types

  DSE 5.0 introduced Geospatial Types. `Point`, `LineString`, and `Polygon` geospatial datatypes can be used from the
  `Dse::Geometry` module. These datatypes can be used in both Cassandra tables and as parameters to DSE Graph.

  Background:
    Given a running dse cluster with schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      USE simplex;
      CREATE TABLE points (k text PRIMARY KEY, v 'PointType');
      CREATE TABLE line_strings (k text PRIMARY KEY, v 'LineStringType');
      CREATE TABLE polygons (k text PRIMARY KEY, v 'PolygonType');
      """

  Scenario: Creating a Point using well-known text (WKT)
    Given the following example:
      """ruby
      require 'dse'

      cluster = Dse.cluster
      session = cluster.connect('simplex')

      session.execute("INSERT INTO points (k, v) VALUES ('point0', 'POINT (3.0 2.0)')")

      result = session.execute('SELECT * FROM points').first
      point = result['v']
      puts "X coordinate: #{point.x}"
      puts "Y coordinate: #{point.y}"
      puts "Coords as string: #{point.to_s}"
      puts "Point's WKT: #{point.wkt}"
      puts ""

      test_point = Dse::Geometry::Point.new('POINT (30 10)')
      puts "X coordinate: #{test_point.x}"
      puts "Y coordinate: #{test_point.y}"
      puts "Coords as string: #{test_point.to_s}"
      puts "Point's WKT: #{test_point.wkt}"
      """
    When it is executed
    Then its output should contain:
      """
      X coordinate: 3.0
      Y coordinate: 2.0
      Coords as string: 3.0,2.0
      Point's WKT: POINT (3.0 2.0)

      X coordinate: 30.0
      Y coordinate: 10.0
      Coords as string: 30.0,10.0
      Point's WKT: POINT (30.0 10.0)
      """

  Scenario: Creating a Point using well-known binary (WKB)
    Given the following example:
      """ruby
      require 'dse'

      cluster = Dse.cluster
      session = cluster.connect('simplex')

      test_point = Dse::Geometry::Point.new(38.0, 21.0)
      session.execute('INSERT INTO points (k, v) VALUES (?, ?)', arguments: ['point1', test_point])

      result = session.execute("SELECT * FROM points WHERE k='point1'").first
      point = result['v']
      puts "X coordinate: #{point.x}"
      puts "Y coordinate: #{point.y}"
      puts "Coords as string: #{point.to_s}"
      puts "Point's WKT: #{point.wkt}"
      puts "Are they the same? #{test_point == point}"
      """
    When it is executed
    Then its output should contain:
      """
      X coordinate: 38.0
      Y coordinate: 21.0
      Coords as string: 38.0,21.0
      Point's WKT: POINT (38.0 21.0)
      Are they the same? true
      """

  Scenario: Creating a LineString using well-known text (WKT)
    Given the following example:
      """ruby
      require 'dse'

      cluster = Dse.cluster
      session = cluster.connect('simplex')

      session.execute("INSERT INTO line_strings (k, v) VALUES ('linestring0', 'LineString (0.0 0.0, 1.0 1.0)')")

      result = session.execute('SELECT * FROM line_strings').first
      line_string = result['v']
      puts "First point: #{line_string.points[0]}"
      puts "Second point: #{line_string.points[1]}"
      puts "LineString as string: #{line_string.to_s}"
      puts "LineString's WKT: #{line_string.wkt}"
      puts ""

      test_line_string = Dse::Geometry::LineString.new('LINESTRING (30 10, 10 30, 40 40)')
      puts "First point: #{test_line_string.points[0]}"
      puts "Second point: #{test_line_string.points[1]}"
      puts "Third point: #{test_line_string.points[2]}"
      puts "LineString as string: #{test_line_string.to_s}"
      puts "LineString's WKT: #{test_line_string.wkt}"
      """
    When it is executed
    Then its output should contain:
      """
      First point: 0.0,0.0
      Second point: 1.0,1.0
      LineString as string: 0.0,0.0 to 1.0,1.0
      LineString's WKT: LINESTRING (0.0 0.0, 1.0 1.0)

      First point: 30.0,10.0
      Second point: 10.0,30.0
      Third point: 40.0,40.0
      LineString as string: 30.0,10.0 to 10.0,30.0 to 40.0,40.0
      LineString's WKT: LINESTRING (30.0 10.0, 10.0 30.0, 40.0 40.0)
      """

  Scenario: Creating a LineString using well-known binary (WKB)
    Given the following example:
      """ruby
      require 'dse'

      cluster = Dse.cluster
      session = cluster.connect('simplex')

      points = [Dse::Geometry::Point.new(2.0, 3.0),
                Dse::Geometry::Point.new(3.0, 4.0),
                Dse::Geometry::Point.new(4.0, 5.0)
      ]

      test_line_string = Dse::Geometry::LineString.new(*points)
      session.execute('INSERT INTO line_strings (k, v) VALUES (?, ?)', arguments: ['linestring1', test_line_string])

      result = session.execute("SELECT * FROM line_strings WHERE k='linestring1'").first
      line_string = result['v']
      puts "First point: #{line_string.points[0]}"
      puts "Second point: #{line_string.points[1]}"
      puts "Third point: #{line_string.points[2]}"
      puts "LineString as string: #{line_string.to_s}"
      puts "LineString's WKT: #{line_string.wkt}"
      puts "Are they the same? #{test_line_string == line_string}"
      """
    When it is executed
    Then its output should contain:
      """
      First point: 2.0,3.0
      Second point: 3.0,4.0
      Third point: 4.0,5.0
      LineString as string: 2.0,3.0 to 3.0,4.0 to 4.0,5.0
      LineString's WKT: LINESTRING (2.0 3.0, 3.0 4.0, 4.0 5.0)
      Are they the same? true
      """

  Scenario: Creating a Polygon using well-known text (WKT)
    Given the following example:
      """ruby
      require 'dse'

      cluster = Dse.cluster
      session = cluster.connect('simplex')

      session.execute("INSERT INTO polygons (k, v) VALUES ('polygon0',
                      'POLYGON (
                                (0.0 0.0, 20.0 0.0, 25.0 25.0, 0.0 25.0, 0.0 0.0),
                                (1.0 1.0, 2.0 2.0, 2.0 1.0, 1.0 1.0)
                      )')"
      )

      result = session.execute("SELECT * FROM polygons WHERE k='polygon0'").first
      polygon = result['v']
      puts "Exterior ring: #{polygon.exterior_ring}"
      puts "Interior rings: #{polygon.interior_rings.map { |linestring| linestring.to_s }}"
      puts "Polygon as string:\n#{polygon.to_s}"
      puts "Polygon's WKT: #{polygon.wkt}"
      puts ""

      test_polygon = Dse::Geometry::Polygon.new('POLYGON ((0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0, 0.0 0.0),
                                               (1.0 1.0, 4.0 9.0, 9.0 1.0, 1.0 1.0))')
      puts "Exterior ring: #{test_polygon.exterior_ring}"
      puts "Interior rings: #{test_polygon.interior_rings.map { |linestring| linestring.to_s }}"
      puts "Polygon as string:\n#{test_polygon.to_s}"
      puts "Polygon's WKT: #{test_polygon.wkt}"
      """
    When it is executed
    Then its output should contain:
      """
      Exterior ring: 0.0,0.0 to 20.0,0.0 to 25.0,25.0 to 0.0,25.0 to 0.0,0.0
      Interior rings: ["1.0,1.0 to 2.0,2.0 to 2.0,1.0 to 1.0,1.0"]
      Polygon as string:
      Exterior ring: 0.0,0.0 to 20.0,0.0 to 25.0,25.0 to 0.0,25.0 to 0.0,0.0
      Interior rings:
          1.0,1.0 to 2.0,2.0 to 2.0,1.0 to 1.0,1.0
      Polygon's WKT: POLYGON ((0.0 0.0, 20.0 0.0, 25.0 25.0, 0.0 25.0, 0.0 0.0), (1.0 1.0, 2.0 2.0, 2.0 1.0, 1.0 1.0))

      Exterior ring: 0.0,0.0 to 10.0,0.0 to 10.0,10.0 to 0.0,10.0 to 0.0,0.0
      Interior rings: ["1.0,1.0 to 4.0,9.0 to 9.0,1.0 to 1.0,1.0"]
      Polygon as string:
      Exterior ring: 0.0,0.0 to 10.0,0.0 to 10.0,10.0 to 0.0,10.0 to 0.0,0.0
      Interior rings:
          1.0,1.0 to 4.0,9.0 to 9.0,1.0 to 1.0,1.0
      Polygon's WKT: POLYGON ((0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0, 0.0 0.0), (1.0 1.0, 4.0 9.0, 9.0 1.0, 1.0 1.0))
      """

  Scenario: Creating a Polygon using well-known binary (WKB)
    Given the following example:
      """ruby
      require 'dse'

      cluster = Dse.cluster
      session = cluster.connect('simplex')

      line_string0 = Dse::Geometry::LineString.new('LINESTRING (0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0, 0.0 0.0)')
      line_string1 = Dse::Geometry::LineString.new('LINESTRING (1.0 1.0, 4.0 9.0, 9.0 1.0, 1.0 1.0)')
      test_polygon = Dse::Geometry::Polygon.new(*[line_string0, line_string1])

      insert = session.prepare('INSERT INTO polygons (k, v) VALUES (?, ?)')
      session.execute(insert, arguments: ['polygon1', test_polygon])

      result = session.execute("SELECT * FROM polygons WHERE k='polygon1'").first
      polygon = result['v']
      puts "Exterior ring: #{polygon.exterior_ring}"
      puts "Interior rings: #{polygon.interior_rings.map { |linestring| linestring.to_s }}"
      puts "Polygon as string:\n#{polygon.to_s}"
      puts "Polygon's WKT: #{polygon.wkt}"
      puts "Are they the same? #{test_polygon == polygon}"
      """
    When it is executed
    Then its output should contain:
      """
      Exterior ring: 0.0,0.0 to 10.0,0.0 to 10.0,10.0 to 0.0,10.0 to 0.0,0.0
      Interior rings: ["1.0,1.0 to 4.0,9.0 to 9.0,1.0 to 1.0,1.0"]
      Polygon as string:
      Exterior ring: 0.0,0.0 to 10.0,0.0 to 10.0,10.0 to 0.0,10.0 to 0.0,0.0
      Interior rings:
          1.0,1.0 to 4.0,9.0 to 9.0,1.0 to 1.0,1.0
      Polygon's WKT: POLYGON ((0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0, 0.0 0.0), (1.0 1.0, 4.0 9.0, 9.0 1.0, 1.0 1.0))
      Are they the same? true
      """

  Scenario: Creating empty LineStrings and Polygons
    Given the following example:
      """ruby
      require 'dse'

      cluster = Dse.cluster
      session = cluster.connect('simplex')

      empty_line_string = Dse::Geometry::LineString.new
      puts "LineString's points: #{empty_line_string.points}"
      puts "LineString's WKT: #{empty_line_string.wkt}"
      puts ""

      empty_polygon = Dse::Geometry::Polygon.new
      puts "Exterior ring nil?: #{empty_polygon.exterior_ring.nil?}"
      puts "Interior rings: #{empty_polygon.interior_rings}"
      puts "Polygon's WKT: #{empty_polygon.wkt}"
      """
    When it is executed
    Then its output should contain:
      """
      LineString's points: []
      LineString's WKT: LINESTRING EMPTY

      Exterior ring nil?: true
      Interior rings: []
      Polygon's WKT: POLYGON EMPTY
      """

