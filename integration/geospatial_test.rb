# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#++

require File.dirname(__FILE__) + '/integration_test_case.rb'
include Dse::Geometry

class GeospatialTest < IntegrationTestCase
  def self.before_suite
    if CCM.dse_version < '5.0.0'
      puts 'DSE > 5.0 required for geospatial tests, skipping setup.'
    else
      super
      @@cluster = Dse.cluster
      @@session = @@cluster.connect

      @@ccm_cluster.setup_schema(<<-SCHEMA)
          CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
          CREATE TABLE simplex.points (name text PRIMARY KEY, point 'PointType');
          CREATE TABLE simplex.line_strings (name text PRIMARY KEY, line_string 'LineStringType');
          CREATE TABLE simplex.polygons (name text PRIMARY KEY, polygon 'PolygonType');
          INSERT INTO simplex.points (name, point) VALUES ('baseline', 'POINT (3.0 2.0)');
          INSERT INTO simplex.line_strings (name, line_string) VALUES ('baseline', 'LineString (3.0 2.0, 4.0 3.0, 5.0 4.0)');
          INSERT INTO simplex.polygons (name, polygon) VALUES ('baseline', 'POLYGON ((0.0 0.0, 20.0 0.0, 25.0 25.0, 0.0 25.0, 0.0 0.0), (1.0 1.0, 2.0 2.0, 2.0 1.0, 1.0 1.0), (5.0 1.0, 7.0 3.0, 7.0 1.0, 5.0 1.0))')
      SCHEMA

      # Adding a sleep here to allow for schema to propagate to all nodes
      sleep(2)
    end
  end

  def self.after_suite
    @@cluster.close unless CCM.dse_version < '5.0.0'
  end

  # Test for inserting and querying a Point.
  #
  # test_good_point tests that the driver can insert a row containing Point data (big-endian) and retrieve it.
  # It also verifies that the baseline row (which was inserted during setup and was inserted little-endian because
  # the test platform is little-endian) is retrieved properly.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-197
  # @expected_result A row should be created with the right Point attributes and the baseline row should also be
  #    retrieved properly.
  #
  # @test_assumptions Dse cluster.
  # @test_category dse:geospatial
  #
  def test_good_point
    skip('Geospatial types are only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    test_point = Point.new(38.0, 21.0)
    @@session.execute('INSERT INTO simplex.points (name, point) VALUES (?, ?)',
                      arguments: ['test', test_point])
    rs = @@session.execute('SELECT * FROM simplex.points')
    rows = {}
    rs.each do |row|
      rows[row['name']] = row['point']
    end
    assert_equal(2, rows.size)
    assert_equal(test_point, rows['test'])
    assert_equal(Point.new(3.0, 2.0), rows['baseline'])
  end

  # Test for inserting and querying a LineString.
  #
  # test_good_line_string tests that the driver can insert a row containing LineString data (big-endian) and retrieve it.
  # It also verifies that the baseline row (which was inserted during setup and was inserted little-endian because
  # the test platform is little-endian) is retrieved properly.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-197
  # @expected_result A row should be created with the right LineString attributes and the baseline row should also be
  #    retrieved properly.
  #
  # @test_assumptions Dse cluster.
  # @test_category dse:geospatial
  #
  def test_good_line_string
    skip('Geospatial types are only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    test_line_string = LineString.new([
                                        Point.new(1.0, 2.0),
                                        Point.new(2.0, 4.0),
                                        Point.new(3.0, 6.0)
                                      ])
    @@session.execute('INSERT INTO simplex.line_strings (name, line_string) VALUES (?, ?)',
                      arguments: ['test', test_line_string])
    rs = @@session.execute('SELECT * FROM simplex.line_strings')
    rows = {}
    rs.each do |row|
      rows[row['name']] = row['line_string']
    end
    assert_equal(2, rows.size)
    assert_equal(test_line_string, rows['test'])
    assert_equal(
      LineString.new([
                       Point.new(3.0, 2.0),
                       Point.new(4.0, 3.0),
                       Point.new(5.0, 4.0)
                     ]),
      rows['baseline'])
  end

  # Test for inserting and querying a Polygon.
  #
  # test_good_polygon tests that the driver can insert a row containing Polygon data (big-endian) and retrieve it.
  # It also verifies that the baseline row (which was inserted during setup and was inserted little-endian because
  # the test platform is little-endian) is retrieved properly.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-197
  # @expected_result A row should be created with the right Polygon attributes and the baseline row should also be
  #    retrieved properly.
  #
  # @test_assumptions Dse cluster.
  # @test_category dse:geospatial
  #
  def test_good_polygon
    skip('Geospatial types are only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    test_polygon = Polygon.new([
                                 LineString.new([
                                                  Point.new(0, 0),
                                                  Point.new(20, 0),
                                                  Point.new(26, 26),
                                                  Point.new(0, 26),
                                                  Point.new(0, 0)
                                                ]),
                                 LineString.new([
                                                  Point.new(1, 1),
                                                  Point.new(1, 5),
                                                  Point.new(5, 5),
                                                  Point.new(5, 1),
                                                  Point.new(1, 1)
                                                ])
                               ])
    @@session.execute('INSERT INTO simplex.polygons (name, polygon) VALUES (?, ?)',
                      arguments: ['test', test_polygon])
    rs = @@session.execute('SELECT * FROM simplex.polygons')
    rows = {}
    rs.each do |row|
      rows[row['name']] = row['polygon']
    end
    assert_equal(2, rows.size)
    assert_equal(test_polygon, rows['test'])
    assert_equal(
      Polygon.new([
                    LineString.new([
                                     Point.new(0.0, 0.0),
                                     Point.new(20.0, 0.0),
                                     Point.new(25.0, 25.0),
                                     Point.new(0.0, 25.0),
                                     Point.new(0.0, 0.0)]),
                    LineString.new([
                                     Point.new(1.0, 1.0),
                                     Point.new(2.0, 2.0),
                                     Point.new(2.0, 1.0),
                                     Point.new(1.0, 1.0)]),
                    LineString.new([
                                     Point.new(5.0, 1.0),
                                     Point.new(7.0, 3.0),
                                     Point.new(7.0, 1.0),
                                     Point.new(5.0, 1.0)])
                  ]),
      rows['baseline'])
  end
end
