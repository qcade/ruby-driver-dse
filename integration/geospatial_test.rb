# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

require File.dirname(__FILE__) + '/integration_test_case.rb'
require 'set'

class GeospatialTest < IntegrationTestCase
  def setup
    if CCM.dse_version >= '5.0.0'
      @@ccm_cluster.setup_schema("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")

      @cluster = Dse.cluster
      @listener = SchemaChangeListener.new(@cluster)
      @session = @cluster.connect('simplex')
    end
  end

  def teardown
    @cluster && @cluster.close
  end

  # Test for inserting and querying a Point
  #
  # test_can_insert_point_type tests that the driver can insert Point datatype and retrieve them. It first creates a
  # simple table which can store Point types. It then performs an insert on this table, creating a row with a simple
  # Point. It then retrieves this point and makes sure all object data is correct. Note that this first Point will be
  # created by DSE internally, as we're simply sending down the WKT down the wire. The test then performs the same
  # steps, except with a pre-defined Point object, which is then passed along to DSE in a prepared statement. Finally,
  # it verifies that a Point can be constructed via its WKT form.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-197
  # @expected_result Point objects should be inserted and properly retrieved in both WKT and WKB
  #
  # @test_category dse:geospatial
  #
  def test_can_insert_point_type
    skip('Geospatial types are only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    @session.execute("CREATE TABLE points (k text PRIMARY KEY, v 'PointType')")

    # point0, inserted via WKT
    @session.execute("INSERT INTO points (k, v) VALUES ('point0', 'POINT (3.0 2.0)')")

    result = @session.execute('SELECT * FROM points').first
    assert_equal 'point0', result['k']
    point = result['v']
    assert_equal 3.0, point.x
    assert_equal 2.0, point.y
    assert_equal '3.0,2.0', point.to_s
    assert_equal 'POINT (3.0 2.0)', point.wkt
    assert_equal Dse::Geometry::Point.new(3.0, 2.0), point

    # point1, inserted via WKB
    test_point = Dse::Geometry::Point.new(38.0, 21.0)
    @session.execute('INSERT INTO points (k, v) VALUES (?, ?)', arguments: ['point1', test_point])

    result = @session.execute("SELECT * FROM points WHERE k='point1'").first
    assert_equal 'point1', result['k']
    point = result['v']
    assert_equal 38.0, point.x
    assert_equal 21.0, point.y
    assert_equal '38.0,21.0', point.to_s
    assert_equal 'POINT (38.0 21.0)', point.wkt
    assert_equal test_point, point

    results = @session.execute('SELECT * FROM points')
    assert_equal 2, results.size

    # point2, constructed via WKT
    test_point = Dse::Geometry::Point.new('POINT (30 10)')
    assert_equal 30.0, test_point.x
    assert_equal 10.0, test_point.y
    assert_equal 'POINT (30.0 10.0)', test_point.wkt
  end

  # Test for edge cases when querying a Point
  #
  # test_point_type_edge_cases tests that the driver handles edge cases when inserting and querying Point datatype. It
  # first creates a simple table to be used by the test. It then validates various valid and invalid Point constructors,
  # both on the server side as well as from the driver. It finally tests some valid and invalid SET/UNSET values.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-197
  # @expected_result Point objects edge cases should be properly handled
  #
  # @test_category dse:geospatial
  #
  def test_point_type_edge_cases
    skip('Geospatial types are only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    @session.execute("CREATE TABLE points (k text PRIMARY KEY, v 'PointType')")

    assert_raises(Cassandra::Errors::InvalidError) do
      @session.execute("INSERT INTO points (k, v) VALUES ('point0', 'POINT ()')")
    end

    # Empty Point is invalid
    assert_raises(Cassandra::Errors::InvalidError) do
      @session.execute("INSERT INTO points (k, v) VALUES ('point0', 'POINT EMPTY')")
    end

    assert_raises(ArgumentError) do
      Dse::Geometry::Point.new('POINT EMPTY')
    end

    assert_raises(ArgumentError) do
      Dse::Geometry::Point.new
    end

    # Null cases
    assert_raises(Cassandra::Errors::InvalidError) do
      @session.execute("INSERT INTO points (k, v) VALUES ('point0', 'POINT (null null)')")
    end

    assert_raises(ArgumentError) do
      Dse::Geometry::Point.new(nil)
    end

    assert_raises(ArgumentError) do
      Dse::Geometry::Point.new('')
    end

    assert_raises(ArgumentError) do
      Dse::Geometry::Point.new('POINT (5 foo)')
    end

    assert_raises(ArgumentError) do
      Dse::Geometry::Point.new(nil, nil)
    end

    assert_raises(ArgumentError) do
      Dse::Geometry::Point.new(Float::NAN, Float::NAN)
    end

    assert_raises(ArgumentError) do
      Dse::Geometry::Point.new(5, 'foo')
    end

    assert_raises(ArgumentError) do
      Dse::Geometry::Point.new(Cassandra::NOT_SET, Cassandra::NOT_SET)
    end

    # Negative points are valid
    test_point = Dse::Geometry::Point.new('POINT (-10 -51.2)')
    assert_equal -10.0, test_point.x
    assert_equal -51.2, test_point.y
    assert_equal 'POINT (-10.0 -51.2)', test_point.wkt

    test_point = Dse::Geometry::Point.new(-10, -51.2)
    assert_equal -10.0, test_point.x
    assert_equal -51.2, test_point.y
    assert_equal 'POINT (-10.0 -51.2)', test_point.wkt

    insert = @session.prepare('INSERT INTO points (k, v) VALUES (?, ?)')
    @session.execute(insert, arguments: ['point0', test_point])

    # Implicit UNSET
    @session.execute(insert, arguments: {'k' => 'point0'})
    assert_equal({"k"=>"point0", "v"=>test_point}, @session.execute("SELECT * FROM points WHERE k='point0'").first)

    # Explicit UNSET
    @session.execute(insert, arguments: ['point0', Cassandra::NOT_SET])
    assert_equal({"k"=>"point0", "v"=>test_point}, @session.execute("SELECT * FROM points WHERE k='point0'").first)

    # Invalid UNSET
    assert_raises(ArgumentError) do
      @session.execute(insert, arguments: [Cassandra::NOT_SET, Dse::Geometry::Point.new(38.0, 21.0)])
    end
  end

  # Test for inserting Point datatype into collections
  #
  # test_can_insert_point_types_into_collections tests that Point datatype can be used in collection datatype, as
  # well as user defined types. It first creates a simple table which can hold all the collection types, as well
  # as a simple UDT which has a Point type as an attribute. It then inserts each collection type with a Point into the
  # table and verifies that the data is retrieved properly.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-197
  # @expected_result Point objects should be inserted and properly retrieved from within collections
  #
  # @test_category dse:geospatial
  #
  def test_can_insert_point_types_into_collections
    skip('Geospatial types are only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    @session.execute("CREATE TYPE udt1 (g 'PointType')")
    @session.execute("CREATE TABLE point_test (k int PRIMARY KEY, l list<'PointType'>, s set<'PointType'>,
                      mk map<'PointType', int>, mv map<int, 'PointType'>, t tuple<'PointType', 'PointType'>,
                      u frozen<udt1>)")
    test_point = Dse::Geometry::Point.new(38.0, 21.0)

    # list
    expected_list = [test_point]
    insert = @session.prepare('INSERT INTO point_test (k, l) VALUES (?, ?)')
    @session.execute(insert, arguments: [0, expected_list])
    retrieved_list = @session.execute('SELECT * FROM point_test').first['l']
    assert_equal expected_list, retrieved_list

    # set
    expected_set = Set.new([test_point])
    insert = @session.prepare('INSERT INTO point_test (k, s) VALUES (?, ?)')
    @session.execute(insert, arguments: [0, expected_set])
    retrieved_set = @session.execute('SELECT * FROM point_test').first['s']
    assert_equal expected_set, retrieved_set

    # map key
    expected_map = {test_point => 0}
    insert = @session.prepare('INSERT INTO point_test (k, mk) VALUES (?, ?)')
    @session.execute(insert, arguments: [0, expected_map])
    retrieved_map = @session.execute('SELECT * FROM point_test').first['mk']
    assert_equal expected_map, retrieved_map

    # map value
    expected_map = {0 => test_point}
    insert = @session.prepare('INSERT INTO point_test (k, mv) VALUES (?, ?)')
    @session.execute(insert, arguments: [0, expected_map])
    retrieved_map = @session.execute('SELECT * FROM point_test').first['mv']
    assert_equal expected_map, retrieved_map

    # tuple
    tuple = Cassandra::Tuple.new(test_point, test_point)
    insert = @session.prepare('INSERT INTO point_test (k, t) VALUES (?, ?)')
    @session.execute(insert, arguments: [0, tuple])
    retrieved_tuple = @session.execute('SELECT * FROM point_test').first['t']
    assert_equal tuple, retrieved_tuple

    # udt
    udt = Cassandra::UDT.new(g: test_point)
    insert = @session.prepare('INSERT INTO point_test (k, u) VALUES (?, ?)')
    @session.execute(insert, arguments: [0, udt])
    retrieved_udt = @session.execute('SELECT * FROM point_test').first['u']
    assert_equal udt, retrieved_udt
  end

  # Test for using Point as PKs and clustering columns
  #
  # test_can_insert_point_type_as_keys tests Point datatype can be used as partition keys and clustering columns. It
  # first creates two tables: one with a Point as the PK, and another with a Point as a clustering column. It then
  # attempts to insert rows into these tables using Point objects as PKs or clustering columns.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-197
  # @expected_result Point objects should be inserted and properly retrieved as PK or clustering columns
  #
  # @test_category dse:geospatial
  #
  def test_can_insert_point_type_as_keys
    skip('Geospatial types are only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    @session.execute("CREATE TABLE pk_test (k 'PointType' PRIMARY KEY, v int)")
    @session.execute("CREATE TABLE clustering_test (k0 text, k1 'PointType', v int, PRIMARY KEY (k0, k1))")
    test_point = Dse::Geometry::Point.new(38.0, 21.0)

    # PK
    insert = @session.prepare('INSERT INTO pk_test (k, v) VALUES (?, ?)')
    @session.execute(insert, arguments: [test_point, 0])
    result = @session.execute('SELECT * FROM pk_test').first
    assert_equal test_point, result['k']
    assert_equal 0, result['v']

    # Clustering
    insert = @session.prepare('INSERT INTO clustering_test (k0, k1, v) VALUES (?, ?, ?)')
    @session.execute(insert, arguments: ['foo', test_point, 0])
    result = @session.execute('SELECT * FROM clustering_test').first
    assert_equal 'foo', result['k0']
    assert_equal test_point, result['k1']
    assert_equal 0, result['v']
  end

  # Test for indexing on Point datatype
  #
  # test_can_index_on_point_type tests Point datatype can be used as an index column, in both secondary indexes and
  # in materialized views. It first creates a simple table with a Point column, and creates a secondary index on that
  # column. It then verifies that the driver retrieves the metadata for this index properly. The same experiment is
  # performed using a materialized view, verifying its metadata is also retrieved properly.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-197
  # @expected_result Point objects can be indexed, and their metadata is retrieved
  #
  # @test_category dse:geospatial
  #
  def test_can_index_on_point_type
    skip('Geospatial types are only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    # Secondary index
    @session.execute("CREATE TABLE index_test (k int PRIMARY KEY, v frozen<map<'PointType', 'PointType'>>)")
    @session.execute("CREATE INDEX v_index ON index_test (full(v))")
    @listener.wait_for_index('simplex', 'index_test', 'v_index')

    assert @cluster.keyspace('simplex').table('index_test').has_index?('v_index')
    index = @cluster.keyspace('simplex').table('index_test').index('v_index')
    assert_equal 'v_index', index.name
    assert_equal 'index_test', index.table.name
    assert_equal :composites, index.kind
    assert_equal 'full(v)', index.target

    # Materialized view
    @session.execute("CREATE TABLE mv_test (k int, v0 'PointType', v1 frozen<map<'PointType', 'PointType'>>,
                      PRIMARY KEY (k, v0))")
    @session.execute("CREATE MATERIALIZED VIEW mv1 AS SELECT v0, v1 FROM mv_test WHERE v0 IS NOT NULL AND v1 IS NOT NULL
                      PRIMARY KEY ((k, v0), v1)")
    @listener.wait_for_materialized_view('simplex', 'mv1')

    assert @cluster.keyspace('simplex').has_materialized_view?('mv1')
    mv_meta = @cluster.keyspace('simplex').materialized_view('mv1')
    assert_equal 'mv1', mv_meta.name
    refute_nil mv_meta.id
    # Temporarily disabled due to RUBY-241
    # assert_equal 'mv_test', mv_meta.base_table.name
    assert_equal 'simplex', mv_meta.keyspace.name
    refute_nil mv_meta.options

    assert_columns([['k', :int], ['v0', :custom], ['v1', :map]], mv_meta.primary_key)
    assert_columns([['k', :int], ['v0', :custom]], mv_meta.partition_key)
    assert_columns([['v1', :map]], mv_meta.clustering_columns)

    assert_equal 3, mv_meta.columns.size
    assert_equal 'k', mv_meta.columns[0].name
    assert_equal :int, mv_meta.columns[0].type.kind
    assert_equal 'v0', mv_meta.columns[1].name
    assert_equal :custom, mv_meta.columns[1].type.kind
    assert_equal 'v1', mv_meta.columns[2].name
    assert_equal :map, mv_meta.columns[2].type.kind
  end

  # Test for inserting and querying a Line String
  #
  # test_can_insert_line_string_type tests that the driver can insert LineString datatype and retrieve them. It first
  # creates a simple table which can store LineString types. It then performs an insert on this table, creating a row
  # with a simple LineString. It then retrieves this LineString and makes sure all object data is correct. Note that
  # this first LineString will be created by DSE internally, as we're simply sending down the WKT down the wire. The
  # test then performs the same steps, except with a pre-defined LineString object, which is then passed along to DSE
  # in a prepared statement. Finally, it verifies that a LineString can be constructed via its WKT form.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-197
  # @expected_result Line String objects should be inserted and properly retrieved in both WKT and WKB
  #
  # @test_category dse:geospatial
  #
  def test_can_insert_line_string_type
    skip('Geospatial types are only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    @session.execute("CREATE TABLE line_strings (k text PRIMARY KEY, v 'LineStringType')")
    @session.execute("INSERT INTO line_strings (k, v) VALUES ('linestring0', 'LineString (0.0 0.0, 1.0 1.0)')")

    # linestring0, inserted via WKT
    points = [Dse::Geometry::Point.new(0.0, 0.0), Dse::Geometry::Point.new(1.0, 1.0)]
    result = @session.execute('SELECT * FROM line_strings').first
    assert_equal 'linestring0', result['k']
    line_string = result['v']
    assert_equal points[0], line_string.points[0]
    assert_equal points[1], line_string.points[1]
    assert_equal '0.0,0.0 to 1.0,1.0', line_string.to_s
    assert_equal 'LINESTRING (0.0 0.0, 1.0 1.0)', line_string.wkt
    assert_equal Dse::Geometry::LineString.new(*points), line_string

    # linestring1, inserted via WKB
    points = [Dse::Geometry::Point.new(2.0, 3.0), Dse::Geometry::Point.new(3.0, 4.0), Dse::Geometry::Point.new(4.0, 5.0)]
    test_line_string = Dse::Geometry::LineString.new(*points)
    @session.execute('INSERT INTO line_strings (k, v) VALUES (?, ?)', arguments: ['linestring1', test_line_string])

    result = @session.execute("SELECT * FROM line_strings WHERE k='linestring1'").first
    assert_equal 'linestring1', result['k']
    line_string = result['v']
    assert_equal points[0], line_string.points[0]
    assert_equal points[1], line_string.points[1]
    assert_equal points[2], line_string.points[2]
    assert_equal '2.0,3.0 to 3.0,4.0 to 4.0,5.0', line_string.to_s
    assert_equal 'LINESTRING (2.0 3.0, 3.0 4.0, 4.0 5.0)', line_string.wkt
    assert_equal test_line_string, line_string

    results = @session.execute('SELECT * FROM line_strings')
    assert_equal 2, results.size

    # linestring2, constructed via WKT
    test_line_string = Dse::Geometry::LineString.new('LINESTRING (30 10, 10 30, 40 40)')
    assert_equal [Dse::Geometry::Point.new(30.0, 10.0), Dse::Geometry::Point.new(10.0, 30.0),
                  Dse::Geometry::Point.new(40.0, 40.0)], test_line_string.points
    assert_equal 'LINESTRING (30.0 10.0, 10.0 30.0, 40.0 40.0)', test_line_string.wkt
  end

  # Test for edge cases when querying a Line String
  #
  # test_line_string_type_edge_cases tests that the driver handles edge cases when inserting and querying Line String
  # datatype. It first creates a simple table to be used by the test. It then validates various valid and invalid Line
  # String constructors, both on the server side as well as from the driver. It finally tests some valid and invalid
  # SET/UNSET values.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-197
  # @expected_result Line String objects edge cases should be properly handled
  #
  # @test_category dse:geospatial
  #
  def test_line_string_type_edge_cases
    skip('Geospatial types are only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    @session.execute("CREATE TABLE line_strings (k text PRIMARY KEY, v 'LineStringType')")

    assert_raises(Cassandra::Errors::InvalidError) do
      @session.execute("INSERT INTO line_strings (k, v) VALUES ('linestring0', 'LINESTRING ()')")
    end

    # EMPTY line string is valid
    @session.execute("INSERT INTO line_strings (k, v) VALUES ('linestring0', 'LINESTRING EMPTY')")
    result = @session.execute("SELECT * FROM line_strings WHERE k='linestring0'").first['v']
    assert_empty result.points
    assert_equal 'LINESTRING EMPTY', result.wkt

    empty_line_string = Dse::Geometry::LineString.new('LINESTRING EMPTY')
    assert_empty empty_line_string.points
    assert_equal 'LINESTRING EMPTY', empty_line_string.wkt

    empty_line_string = Dse::Geometry::LineString.new
    assert_empty empty_line_string.points
    assert_equal 'LINESTRING EMPTY', empty_line_string.wkt

    # line string with a single Point is invalid
    assert_raises(Cassandra::Errors::InvalidError) do
      @session.execute("INSERT INTO line_strings (k, v) VALUES ('linestring3', 'LINESTRING (2.0 3.0)')")
    end

    assert_raises(ArgumentError) do
      Dse::Geometry::LineString.new(Dse::Geometry::Point.new(0.0, 0.0))
    end

    # line string with two of the same Point is invalid
    points = [Dse::Geometry::Point.new(0.0, 0.0), Dse::Geometry::Point.new(0.0, 0.0)]
    test_line_string = Dse::Geometry::LineString.new(*points)
    assert_raises(Cassandra::Errors::InvalidError) do
      @session.execute("INSERT INTO line_strings (k, v) VALUES ('linestring3', ?)", arguments: [test_line_string])
    end

    # Null cases
    assert_raises(ArgumentError) do
      Dse::Geometry::LineString.new(nil)
    end

    assert_raises(ArgumentError) do
      Dse::Geometry::LineString.new('')
    end

    assert_raises(ArgumentError) do
      Dse::Geometry::LineString.new('LINESTRING (1.0 2.0, foo bar)')
    end

    assert_raises(ArgumentError) do
      Dse::Geometry::LineString.new(*[nil, nil])
    end

    assert_raises(ArgumentError) do
      Dse::Geometry::LineString.new(*[Dse::Geometry::Point.new(0.0, 0.0), 'foo', 5])
    end

    assert_raises(ArgumentError) do
      Dse::Geometry::LineString.new(*[Cassandra::NOT_SET, Cassandra::NOT_SET])
    end

    # Line string with negative Points are valid
    points = [Dse::Geometry::Point.new(-5.5, -12.3), Dse::Geometry::Point.new(-10.0, 3.1)]
    test_line_string = Dse::Geometry::LineString.new(*points)
    assert_equal 'LINESTRING (-5.5 -12.3, -10.0 3.1)', test_line_string.wkt

    insert = @session.prepare('INSERT INTO line_strings (k, v) VALUES (?, ?)')
    @session.execute(insert, arguments: ['linestring4', test_line_string])

    # Implicit UNSET
    @session.execute(insert, arguments: {'k' => 'linestring4'})
    assert_equal({"k"=>"linestring4", "v"=>test_line_string},
                 @session.execute("SELECT * FROM line_strings WHERE k='linestring4'").first)

    # Explicit UNSET
    @session.execute(insert, arguments: ['linestring4', Cassandra::NOT_SET])
    assert_equal({"k"=>"linestring4", "v"=>test_line_string},
                 @session.execute("SELECT * FROM line_strings WHERE k='linestring4'").first)

    # Invalid UNSET
    assert_raises(ArgumentError) do
      @session.execute(insert, arguments: [Cassandra::NOT_SET, test_line_string])
    end
  end

  # Test for inserting Line String datatype into collections
  #
  # test_can_insert_line_string_types_into_collections tests that Line String datatype can be used in collection
  # datatype, as well as user defined types. It first creates a simple table which can hold all the collection types,
  # as well as a simple UDT which has a Line String type as an attribute. It then inserts each collection type with a
  # Line String into the table and verifies that the data is retrieved properly.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-197
  # @expected_result Line String objects should be inserted and properly retrieved from within collections
  #
  # @test_category dse:geospatial
  #
  def test_can_insert_line_string_types_into_collections
    skip('Geospatial types are only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    @session.execute("CREATE TYPE udt1 (g 'LineStringType')")
    @session.execute("CREATE TABLE linestring_test (k int PRIMARY KEY, l list<'LineStringType'>, s set<'LineStringType'>,
                      mk map<'LineStringType', int>, mv map<int, 'LineStringType'>,
                      t tuple<'LineStringType', 'LineStringType'>, u frozen<udt1>)")
    points = [Dse::Geometry::Point.new(2.0, 3.0), Dse::Geometry::Point.new(3.0, 4.0)]
    test_line_string = Dse::Geometry::LineString.new(*points)

    # list
    expected_list = [test_line_string]
    insert = @session.prepare('INSERT INTO linestring_test (k, l) VALUES (?, ?)')
    @session.execute(insert, arguments: [0, expected_list])
    retrieved_list = @session.execute('SELECT * FROM linestring_test').first['l']
    assert_equal expected_list, retrieved_list

    # set
    expected_set = Set.new([test_line_string])
    insert = @session.prepare('INSERT INTO linestring_test (k, s) VALUES (?, ?)')
    @session.execute(insert, arguments: [0, expected_set])
    retrieved_set = @session.execute('SELECT * FROM linestring_test').first['s']
    assert_equal expected_set, retrieved_set

    # map key
    expected_map = {test_line_string => 0}
    insert = @session.prepare('INSERT INTO linestring_test (k, mk) VALUES (?, ?)')
    @session.execute(insert, arguments: [0, expected_map])
    retrieved_map = @session.execute('SELECT * FROM linestring_test').first['mk']
    assert_equal expected_map, retrieved_map

    # map value
    expected_map = {0 => test_line_string}
    insert = @session.prepare('INSERT INTO linestring_test (k, mv) VALUES (?, ?)')
    @session.execute(insert, arguments: [0, expected_map])
    retrieved_map = @session.execute('SELECT * FROM linestring_test').first['mv']


    # tuple
    tuple = Cassandra::Tuple.new(test_line_string, test_line_string)
    insert = @session.prepare('INSERT INTO linestring_test (k, t) VALUES (?, ?)')
    @session.execute(insert, arguments: [0, tuple])
    retrieved_tuple = @session.execute('SELECT * FROM linestring_test').first['t']
    assert_equal tuple, retrieved_tuple

    # udt
    udt = Cassandra::UDT.new(g: test_line_string)
    insert = @session.prepare('INSERT INTO linestring_test (k, u) VALUES (?, ?)')
    @session.execute(insert, arguments: [0, udt])
    retrieved_udt = @session.execute('SELECT * FROM linestring_test').first['u']
    assert_equal udt, retrieved_udt
  end

  # Test for using Line String as PKs and clustering columns
  #
  # test_can_insert_line_string_type_as_keys tests Line String datatype can be used as partition keys and clustering
  # columns. It first creates two tables: one with a Line String as the PK, and another with a Line String as a
  # clustering column. It then attempts to insert rows into these tables using Line String objects as PKs or clustering
  # columns.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-197
  # @expected_result Line String objects should be inserted and properly retrieved as PK or clustering columns
  #
  # @test_category dse:geospatial
  #
  def test_can_insert_line_string_type_as_keys
    skip('Geospatial types are only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    @session.execute("CREATE TABLE pk_test2 (k 'LineStringType' PRIMARY KEY, v int)")
    @session.execute("CREATE TABLE clustering_test2 (k0 text, k1 'LineStringType', v int, PRIMARY KEY (k0, k1))")
    points = [Dse::Geometry::Point.new(2.0, 3.0), Dse::Geometry::Point.new(3.0, 4.0)]
    test_line_string = Dse::Geometry::LineString.new(*points)

    # PK
    insert = @session.prepare('INSERT INTO pk_test2 (k, v) VALUES (?, ?)')
    @session.execute(insert, arguments: [test_line_string, 0])
    result = @session.execute('SELECT * FROM pk_test2').first
    assert_equal test_line_string, result['k']
    assert_equal 0, result['v']

    # Clustering
    insert = @session.prepare('INSERT INTO clustering_test2 (k0, k1, v) VALUES (?, ?, ?)')
    @session.execute(insert, arguments: ['foo', test_line_string, 0])
    result = @session.execute('SELECT * FROM clustering_test2').first
    assert_equal 'foo', result['k0']
    assert_equal test_line_string, result['k1']
    assert_equal 0, result['v']
  end

  # Test for indexing on Line String datatype
  #
  # test_can_index_on_line_string_type tests Line String datatype can be used as an index column, in both secondary
  # indexes and in materialized views. It first creates a simple table with a Line String column, and creates a
  # secondary index on that column. It then verifies that the driver retrieves the metadata for this index properly.
  # The same experiment is performed using a materialized view, verifying its metadata is also retrieved properly.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-197
  # @expected_result Line String objects can be indexed, and their metadata is retrieved
  #
  # @test_category dse:geospatial
  #
  def test_can_index_on_line_string_type
    skip('Geospatial types are only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    # Secondary index
    @session.execute("CREATE TABLE index_test2 (k int PRIMARY KEY, v frozen<map<'LineStringType', 'LineStringType'>>)")
    @session.execute("CREATE INDEX v_index ON index_test2 (full(v))")
    @listener.wait_for_index('simplex', 'index_test2', 'v_index')

    assert @cluster.keyspace('simplex').table('index_test2').has_index?('v_index')
    index = @cluster.keyspace('simplex').table('index_test2').index('v_index')
    assert_equal 'v_index', index.name
    assert_equal 'index_test2', index.table.name
    assert_equal :composites, index.kind
    assert_equal 'full(v)', index.target

    # Materialized view
    @session.execute("CREATE TABLE mv_test2 (k int, v0 'LineStringType', v1 frozen<map<'LineStringType', 'LineStringType'>>,
                      PRIMARY KEY (k, v0))")
    @session.execute("CREATE MATERIALIZED VIEW mv2 AS SELECT v0, v1 FROM mv_test2 WHERE v0 IS NOT NULL AND v1 IS NOT NULL
                      PRIMARY KEY ((k, v0), v1)")
    @listener.wait_for_materialized_view('simplex', 'mv2')

    assert @cluster.keyspace('simplex').has_materialized_view?('mv2')
    mv_meta = @cluster.keyspace('simplex').materialized_view('mv2')
    assert_equal 'mv2', mv_meta.name
    refute_nil mv_meta.id
    # Temporarily disabled due to RUBY-241
    # assert_equal 'mv_test2', mv_meta.base_table.name
    assert_equal 'simplex', mv_meta.keyspace.name
    refute_nil mv_meta.options

    assert_columns([['k', :int], ['v0', :custom], ['v1', :map]], mv_meta.primary_key)
    assert_columns([['k', :int], ['v0', :custom]], mv_meta.partition_key)
    assert_columns([['v1', :map]], mv_meta.clustering_columns)

    assert_equal 3, mv_meta.columns.size
    assert_equal 'k', mv_meta.columns[0].name
    assert_equal :int, mv_meta.columns[0].type.kind
    assert_equal 'v0', mv_meta.columns[1].name
    assert_equal :custom, mv_meta.columns[1].type.kind
    assert_equal 'v1', mv_meta.columns[2].name
    assert_equal :map, mv_meta.columns[2].type.kind
  end

  # Test for inserting and querying a Polygon
  #
  # test_can_insert_polygon_type tests that the driver can insert Polygon datatype and retrieve them. It first creates
  # a simple table which can store Polygon types. It then performs an insert on this table, creating a row with a simple
  # Polygon. It then retrieves this Polygon and makes sure all object data is correct. Note that this first Polygon
  # will be created by DSE internally, as we're simply sending down the WKT down the wire. The test then performs the
  # same steps, except with a pre-defined Polygon object, which is then passed along to DSE in a prepared statement.
  # Finally, it verifies that a Polygon can be constructed via its WKT form.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-197
  # @expected_result Polygon objects should be inserted and properly retrieved in both WKT and WKB
  #
  # @test_category dse:geospatial
  #
  def test_can_insert_polygon_type
    skip('Geospatial types are only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    @session.execute("CREATE TABLE polygons (k text PRIMARY KEY, v 'PolygonType')")
    @session.execute("INSERT INTO polygons (k, v) VALUES ('polygon0', 'POLYGON (
                      (0.0 0.0, 20.0 0.0, 25.0 25.0, 0.0 25.0, 0.0 0.0), (1.0 1.0, 2.0 2.0, 2.0 1.0, 1.0 1.0))')")

    # polygon0, inserted via WKT
    line_string0 = Dse::Geometry::LineString.new('LINESTRING (0.0 0.0, 20.0 0.0, 25.0 25.0, 0.0 25.0, 0.0 0.0)')
    line_string1 = Dse::Geometry::LineString.new('LINESTRING (1.0 1.0, 2.0 2.0, 2.0 1.0, 1.0 1.0)')
    result = @session.execute('SELECT * FROM polygons').first
    assert_equal 'polygon0', result['k']
    polygon = result['v']
    assert_equal line_string0, polygon.exterior_ring
    assert_equal [line_string1], polygon.interior_rings
    assert_match "Exterior ring: 0.0,0.0 to 20.0,0.0 to 25.0,25.0 to 0.0,25.0 to 0.0,0.0\nInterior rings:\
\n    1.0,1.0 to 2.0,2.0 to 2.0,1.0 to 1.0,1.0", polygon.to_s
    assert_equal 'POLYGON ((0.0 0.0, 20.0 0.0, 25.0 25.0, 0.0 25.0, 0.0 0.0), (1.0 1.0, 2.0 2.0, 2.0 1.0, 1.0 1.0))',
                 polygon.wkt
    assert_equal Dse::Geometry::Polygon.new(*[line_string0, line_string1]), polygon

    # polygon1, inserted via WKB
    line_string0 = Dse::Geometry::LineString.new('LINESTRING (0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0, 0.0 0.0)')
    line_string1 = Dse::Geometry::LineString.new('LINESTRING (1.0 1.0, 4.0 9.0, 9.0 1.0, 1.0 1.0)')
    test_polygon = Dse::Geometry::Polygon.new(*[line_string0, line_string1])

    insert = @session.prepare('INSERT INTO polygons (k, v) VALUES (?, ?)')
    @session.execute(insert, arguments: ['polygon1', test_polygon])

    result = @session.execute("SELECT * FROM polygons WHERE k='polygon1'").first
    assert_equal 'polygon1', result['k']
    polygon = result['v']
    assert_equal line_string0, polygon.exterior_ring
    assert_equal [line_string1], polygon.interior_rings
    assert_match "Exterior ring: 0.0,0.0 to 10.0,0.0 to 10.0,10.0 to 0.0,10.0 to 0.0,0.0\nInterior rings:\
\n    1.0,1.0 to 4.0,9.0 to 9.0,1.0 to 1.0,1.0", polygon.to_s
    assert_equal 'POLYGON ((0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0, 0.0 0.0), (1.0 1.0, 4.0 9.0, 9.0 1.0, 1.0 1.0))',
                 polygon.wkt
    assert_equal test_polygon, polygon

    results = @session.execute('SELECT * FROM polygons')
    assert_equal 2, results.size

    # polygon2, constructed via WKT
    test_polygon2 = Dse::Geometry::Polygon.new('POLYGON ((0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0, 0.0 0.0),
                                               (1.0 1.0, 4.0 9.0, 9.0 1.0, 1.0 1.0))')
    assert_equal line_string0, test_polygon2.exterior_ring
    assert_equal [line_string1], test_polygon2.interior_rings
    assert_equal 'POLYGON ((0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0, 0.0 0.0), (1.0 1.0, 4.0 9.0, 9.0 1.0, 1.0 1.0))',
                 test_polygon2.wkt
    assert_equal test_polygon, test_polygon2
  end

  # Test for edge cases when querying a Polygon
  #
  # test_polygon_type_edge_cases tests that the driver handles edge cases when inserting and querying the Polygon
  # datatype. It first creates a simple table to be used by the test. It then validates various valid and invalid
  # Polygon constructors, both on the server side as well as from the driver. It finally tests some valid and invalid
  # SET/UNSET values.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-197
  # @expected_result Polygon object edge cases should be properly handled
  #
  # @test_category dse:geospatial
  #
  def test_polygon_type_edge_cases
    skip('Geospatial types are only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    @session.execute("CREATE TABLE polygons (k text PRIMARY KEY, v 'PolygonType')")

    assert_raises(Cassandra::Errors::InvalidError) do
      @session.execute("INSERT INTO polygons (k, v) VALUES ('polygon0', 'PolygonType ()')")
    end

    # EMPTY polygon is valid
    @session.execute("INSERT INTO polygons (k, v) VALUES ('polygon0', 'POLYGON EMPTY')")
    result = @session.execute("SELECT * FROM polygons WHERE k='polygon0'").first['v']
    assert_nil result.exterior_ring
    assert_empty result.interior_rings
    assert_equal 'POLYGON EMPTY', result.wkt

    empty_polygon = Dse::Geometry::Polygon.new('POLYGON EMPTY')
    assert_nil empty_polygon.exterior_ring
    assert_empty empty_polygon.interior_rings
    assert_equal 'POLYGON EMPTY', empty_polygon.wkt

    empty_polygon = Dse::Geometry::Polygon.new
    assert_nil empty_polygon.exterior_ring
    assert_empty empty_polygon.interior_rings
    assert_equal 'POLYGON EMPTY', empty_polygon.wkt

    # polygon with Line String of 2 points is invalid
    test_line = Dse::Geometry::LineString.new('LINESTRING (0.0 0.0, 1.0 1.0)')
    test_polygon = Dse::Geometry::Polygon.new(*[test_line])
    assert_raises(Cassandra::Errors::InvalidError) do
      @session.execute("INSERT INTO polygons (k, v) VALUES ('polygon1', ?)", arguments: [test_polygon])
    end

    # polygon with a Line String of 3 points is valid (DSE will insert the 4th point in WKT)
    @session.execute("INSERT INTO polygons (k, v) VALUES ('polygon1', 'POLYGON ((0.0 0.0, 1.0 0.0, 1.0 1.0))')")
    result = @session.execute("SELECT * FROM polygons WHERE k='polygon1'").first['v']
    assert_equal Dse::Geometry::LineString.new('LINESTRING (0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0)'), result.exterior_ring
    assert_empty result.interior_rings
    assert_equal 'POLYGON ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))', result.wkt

    # polygon with a Line String of 3 points is invalid (WKB)
    test_line = Dse::Geometry::LineString.new('LINESTRING (0.0 0.0, 1.0 0.0, 1.0 1.0)')
    test_polygon = Dse::Geometry::Polygon.new(*[test_line])
    assert_raises(Cassandra::Errors::InvalidError) do
      @session.execute("INSERT INTO polygons (k, v) VALUES ('polygon2', ?)", arguments: [test_polygon])
    end

    # polygon that doesn't close is valid (DSE will insert the closing point in WKT)
    @session.execute("INSERT INTO polygons (k, v) VALUES ('polygon2',
                     'POLYGON ((0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0))')"
    )
    result = @session.execute("SELECT * FROM polygons WHERE k='polygon2'").first['v']
    assert_equal Dse::Geometry::LineString.new('LINESTRING (0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0, 0.0 0.0)'),
                 result.exterior_ring
    assert_empty result.interior_rings
    assert_equal 'POLYGON ((0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0, 0.0 0.0))', result.wkt

    # polygon that doesn't close is invalid (WKB)
    test_line = Dse::Geometry::LineString.new('LINESTRING (0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0)')
    test_polygon = Dse::Geometry::Polygon.new(*[test_line])
    assert_raises(Cassandra::Errors::InvalidError) do
      @session.execute("INSERT INTO polygons (k, v) VALUES ('polygon3', ?)", arguments: [test_polygon])
    end

    # polygon with incorrect ring directions is valid (DSE will reverse the ring directions in WKT)
    @session.execute("INSERT INTO polygons (k, v) VALUES ('polygon3',
                      'POLYGON ((0.0 0.0, 0.0 10.0, 10.0 10.0, 10.0 0.0, 0.0 0.0),
                      (1.0 1.0, 9.0 1.0, 4.0 9.0, 1.0 1.0))')"
    )
    result = @session.execute("SELECT * FROM polygons WHERE k='polygon3'").first['v']
    assert_equal Dse::Geometry::LineString.new('LINESTRING (0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0, 0.0 0.0)'),
                 result.exterior_ring
    assert_equal [Dse::Geometry::LineString.new('LINESTRING (1.0 1.0, 4.0 9.0, 9.0 1.0, 1.0 1.0)')],
                 result.interior_rings
    assert_equal 'POLYGON ((0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0, 0.0 0.0), (1.0 1.0, 4.0 9.0, 9.0 1.0, 1.0 1.0))',
                 result.wkt

    # polygon with incorrect ring directions is invalid (WKB)
    line_string0 = Dse::Geometry::LineString.new('LINESTRING (0.0 0.0, 0.0 10.0, 10.0 10.0, 10.0 0.0, 0.0 0.0)')
    line_string1 = Dse::Geometry::LineString.new('LINESTRING (1.0 1.0, 9.0 1.0, 4.0 9.0, 1.0 1.0)')
    test_polygon = Dse::Geometry::Polygon.new(*[line_string0, line_string1])
    assert_raises(Cassandra::Errors::InvalidError) do
      @session.execute("INSERT INTO polygons (k, v) VALUES ('polygon4', ?)", arguments: [test_polygon])
    end

    # Null cases
    assert_raises(ArgumentError) do
      Dse::Geometry::Polygon.new(nil)
    end

    assert_raises(ArgumentError) do
      Dse::Geometry::Polygon.new('')
    end

    assert_raises(ArgumentError) do
      Dse::Geometry::Polygon.new('POLYGON ((0.0 0.0, foo 0.0, 10.0 10.0, bar 10.0))')
    end

    assert_raises(ArgumentError) do
      Dse::Geometry::Polygon.new(*[Dse::Geometry::LineString.new('LINESTRING (0.0 0.0, 1.0 1.0)'), nil])
    end

    assert_raises(ArgumentError) do
      Dse::Geometry::Polygon.new(*[Dse::Geometry::LineString.new('LINESTRING (0.0 0.0, 1.0 1.0)'), Cassandra::NOT_SET])
    end

    # Polygon that reach negative space is valid
    test_polygon = Dse::Geometry::Polygon.new('POLYGON ((2.0 0.0, 2.0 2.0, -2.0 2.0, -2.0 -2.0, 2.0 -2.0, 2.0 0.0))')
    assert_equal 'POLYGON ((2.0 0.0, 2.0 2.0, -2.0 2.0, -2.0 -2.0, 2.0 -2.0, 2.0 0.0))', test_polygon.wkt

    insert = @session.prepare('INSERT INTO polygons (k, v) VALUES (?, ?)')
    @session.execute(insert, arguments: ['polygon4', test_polygon])

    # Implicit UNSET
    @session.execute(insert, arguments: {'k' => 'polygon4'})
    assert_equal({"k"=>"polygon4", "v"=>test_polygon},
                 @session.execute("SELECT * FROM polygons WHERE k='polygon4'").first)

    # Explicit UNSET
    @session.execute(insert, arguments: ['polygon4', Cassandra::NOT_SET])
    assert_equal({"k"=>"polygon4", "v"=>test_polygon},
                 @session.execute("SELECT * FROM polygons WHERE k='polygon4'").first)

    # Invalid UNSET
    assert_raises(ArgumentError) do
      @session.execute(insert, arguments: [Cassandra::NOT_SET, test_polygon])
    end
  end

  # Test for inserting Polygon datatype into collections
  #
  # test_can_insert_polygon_types_into_collections tests that the Polygon datatype can be used in collection
  # datatypes, as well as user defined types. It first creates a simple table which can hold all the collection types,
  # as well as a simple UDT which has a Polygon type as an attribute. It then inserts each collection type with a
  # Polygon into the table and verifies that the data is retrieved properly.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-197
  # @expected_result Polygon objects should be inserted and properly retrieved from within collections
  #
  # @test_category dse:geospatial
  #
  def test_can_insert_polygon_types_into_collections
    skip('Geospatial types are only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    @session.execute("CREATE TYPE udt1 (g 'PolygonType')")
    @session.execute("CREATE TABLE polygon_test (k int PRIMARY KEY, l list<'PolygonType'>, s set<'PolygonType'>,
                      mk map<'PolygonType', int>, mv map<int, 'PolygonType'>,
                      t tuple<'PolygonType', 'PolygonType'>, u frozen<udt1>)")
    test_polygon = Dse::Geometry::Polygon.new('POLYGON ((0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0, 0.0 0.0),
                                               (1.0 1.0, 4.0 9.0, 9.0 1.0, 1.0 1.0))')

    # list
    expected_list = [test_polygon]
    insert = @session.prepare('INSERT INTO polygon_test (k, l) VALUES (?, ?)')
    @session.execute(insert, arguments: [0, expected_list])
    retrieved_list = @session.execute('SELECT * FROM polygon_test').first['l']
    assert_equal expected_list, retrieved_list

    # set
    expected_set = Set.new([test_polygon])
    insert = @session.prepare('INSERT INTO polygon_test (k, s) VALUES (?, ?)')
    @session.execute(insert, arguments: [0, expected_set])
    retrieved_set = @session.execute('SELECT * FROM polygon_test').first['s']
    assert_equal expected_set, retrieved_set

    # map key
    expected_map = {test_polygon => 0}
    insert = @session.prepare('INSERT INTO polygon_test (k, mk) VALUES (?, ?)')
    @session.execute(insert, arguments: [0, expected_map])
    retrieved_map = @session.execute('SELECT * FROM polygon_test').first['mk']
    assert_equal expected_map, retrieved_map

    # map value
    expected_map = {0 => test_polygon}
    insert = @session.prepare('INSERT INTO polygon_test (k, mv) VALUES (?, ?)')
    @session.execute(insert, arguments: [0, expected_map])
    retrieved_map = @session.execute('SELECT * FROM polygon_test').first['mv']
    assert_equal expected_map, retrieved_map

    # tuple
    tuple = Cassandra::Tuple.new(test_polygon, test_polygon)
    insert = @session.prepare('INSERT INTO polygon_test (k, t) VALUES (?, ?)')
    @session.execute(insert, arguments: [0, tuple])
    retrieved_tuple = @session.execute('SELECT * FROM polygon_test').first['t']
    assert_equal tuple, retrieved_tuple

    # udt
    udt = Cassandra::UDT.new(g: test_polygon)
    insert = @session.prepare('INSERT INTO polygon_test (k, u) VALUES (?, ?)')
    @session.execute(insert, arguments: [0, udt])
    retrieved_udt = @session.execute('SELECT * FROM polygon_test').first['u']
    assert_equal udt, retrieved_udt
  end

  # Test for using Polygon as PKs and clustering columns
  #
  # test_can_insert_polygon_type_as_keys tests Polygon datatype can be used as partition keys and clustering
  # columns. It first creates two tables: one with a Polygon as the PK, and another with a Polygon as a
  # clustering column. It then attempts to insert rows into these tables using Polygon objects as PKs or clustering
  # columns.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-197
  # @expected_result Polygon objects should be inserted and properly retrieved as PK or clustering columns
  #
  # @test_category dse:geospatial
  #
  def test_can_insert_polygon_type_as_keys
    skip('Geospatial types are only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    @session.execute("CREATE TABLE pk_test3 (k 'PolygonType' PRIMARY KEY, v int)")
    @session.execute("CREATE TABLE clustering_test3 (k0 text, k1 'PolygonType', v int, PRIMARY KEY (k0, k1))")
    test_polygon = Dse::Geometry::Polygon.new('POLYGON ((0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0, 0.0 0.0),
                                               (1.0 1.0, 4.0 9.0, 9.0 1.0, 1.0 1.0))')

    # PK
    insert = @session.prepare('INSERT INTO pk_test3 (k, v) VALUES (?, ?)')
    @session.execute(insert, arguments: [test_polygon, 0])
    result = @session.execute('SELECT * FROM pk_test3').first
    assert_equal test_polygon, result['k']
    assert_equal 0, result['v']

    # Clustering
    insert = @session.prepare('INSERT INTO clustering_test3 (k0, k1, v) VALUES (?, ?, ?)')
    @session.execute(insert, arguments: ['foo', test_polygon, 0])
    result = @session.execute('SELECT * FROM clustering_test3').first
    assert_equal 'foo', result['k0']
    assert_equal test_polygon, result['k1']
    assert_equal 0, result['v']
  end

  # Test for indexing on Polygon datatype
  #
  # test_can_index_on_polygon_type tests Polygon datatype can be used as an index column, in both secondary
  # indexes and in materialized views. It first creates a simple table with a Polygon column, and creates a
  # secondary index on that column. It then verifies that the driver retrieves the metadata for this index properly.
  # The same experiment is performed using a materialized view, verifying its metadata is also retrieved properly.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-197
  # @expected_result Polygon objects can be indexed, and their metadata is retrieved
  #
  # @test_category dse:geospatial
  #
  def test_can_index_on_polygon_type
    skip('Geospatial types are only available in DSE after 5.0') if CCM.dse_version < '5.0.0'
    
    # Secondary index
    @session.execute("CREATE TABLE index_test3 (k int PRIMARY KEY, v frozen<map<'PolygonType', 'PolygonType'>>)")
    @session.execute("CREATE INDEX v_index ON index_test3 (full(v))")
    @listener.wait_for_index('simplex', 'index_test3', 'v_index')

    assert @cluster.keyspace('simplex').table('index_test3').has_index?('v_index')
    index = @cluster.keyspace('simplex').table('index_test3').index('v_index')
    assert_equal 'v_index', index.name
    assert_equal 'index_test3', index.table.name
    assert_equal :composites, index.kind
    assert_equal 'full(v)', index.target

    # Materialized view
    @session.execute("CREATE TABLE mv_test3 (k int, v0 'PolygonType', v1 frozen<map<'PolygonType', 'PolygonType'>>,
                      PRIMARY KEY (k, v0))")
    @session.execute("CREATE MATERIALIZED VIEW mv3 AS SELECT v0, v1 FROM mv_test3 WHERE v0 IS NOT NULL AND v1 IS NOT NULL
                      PRIMARY KEY ((k, v0), v1)")
    @listener.wait_for_materialized_view('simplex', 'mv3')

    assert @cluster.keyspace('simplex').has_materialized_view?('mv3')
    mv_meta = @cluster.keyspace('simplex').materialized_view('mv3')
    assert_equal 'mv3', mv_meta.name
    refute_nil mv_meta.id
    # Temporarily disabled due to RUBY-241
    # assert_equal 'mv_test3', mv_meta.base_table.name
    assert_equal 'simplex', mv_meta.keyspace.name
    refute_nil mv_meta.options

    assert_columns([['k', :int], ['v0', :custom], ['v1', :map]], mv_meta.primary_key)
    assert_columns([['k', :int], ['v0', :custom]], mv_meta.partition_key)
    assert_columns([['v1', :map]], mv_meta.clustering_columns)

    assert_equal 3, mv_meta.columns.size
    assert_equal 'k', mv_meta.columns[0].name
    assert_equal :int, mv_meta.columns[0].type.kind
    assert_equal 'v0', mv_meta.columns[1].name
    assert_equal :custom, mv_meta.columns[1].type.kind
    assert_equal 'v1', mv_meta.columns[2].name
    assert_equal :map, mv_meta.columns[2].type.kind
  end
end
