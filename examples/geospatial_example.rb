# This is temporary while we're getting the Ruby driver from GitHub.
require 'bundler'
Bundler.setup

require 'dse'

# The geospatial types are defined in the Dse::Geometry module. Save some typing and include it here so that
# we can refer to the classes with their base names.
include Dse::Geometry

cluster = Dse.cluster
session = cluster.connect(keyspace: 'simplex')

########## Point ##############
puts "==== Point ====\n"

# Create a table with a PointType column and insert a row into it.
session.execute("CREATE TABLE IF NOT EXISTS points_of_interest (name text PRIMARY KEY, coords 'PointType')")
session.execute('INSERT INTO points_of_interest (name, coords) VALUES (?, ?)',
                arguments: ['Empire State', Point.new(38.0, 21.0)])

# Now retrieve the point.
rs = session.execute('SELECT * FROM points_of_interest')
rs.each do |row|
  # We can emit the point in its WKT (https://en.wikipedia.org/wiki/Well-known_text) representation.
  puts "#{row['name']}   #{row['coords'].wkt}"

  # Or the x and y coordinates
  puts "#{row['name']}   #{row['coords'].x},#{row['coords'].y}"

  # Which is really the to_s of the point, so you can do this:
  puts "#{row['name']}   #{row['coords']}"
end

########## LineString ##############
puts "\n==== LineString ====\n"

# Create a table with a LineString column and insert a row into it. A LineString is an ordered collection of points;
# connect the dots to get the line-string!
session.execute("CREATE TABLE IF NOT EXISTS directions (origin text PRIMARY KEY, destination text, directions 'LineStringType')")
session.execute('INSERT INTO directions (origin, destination, directions) VALUES (?, ?, ?)',
                arguments: ['office', 'home', LineString.new([
                                                                 Point.new(12.0, 21.0),
                                                                 Point.new(13.0, 31.0),
                                                                 Point.new(14.0, 41.0)
                                                             ])])
# Now retrieve the line-string.
rs = session.execute('SELECT * FROM directions')
rs.each do |row|
  directions = row['directions'].points.map do |point|
    "#{point.x},#{point.y}"
  end.join(" to ")
  puts "Directions from #{row['origin']} to #{row['destination']}: #{directions}"
  # Or more simply (thanks to an overridden to_s)
  puts "Directions from #{row['origin']} to #{row['destination']}: #{row['directions']}"
  # And its wkt for fun
  puts "WKT: #{row['directions'].wkt}"
end

########## Polygon ##############
puts "\n==== Polygon ====\n"

# Create a table with a Polygon column and insert a row into it. A polygon consists of a set of linear-rings.
# A linear-ring is a LineString whose last point is the same as its first point.
#
# The first ring specified in a polygon defines the outer edges of the polygon and is called the 'exterior ring'.
# A polygon may also have "holes" within it, specified by other linear rings, and those holes may contain
# linear-rings indicating "islands". All such rings are called 'interior rings'.

session.execute("CREATE TABLE IF NOT EXISTS places (name text PRIMARY KEY, layout 'PolygonType')")
session.execute('INSERT INTO places (name, layout) VALUES (?, ?)',
                arguments: ['Capitol', Polygon.new([
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
                                                   ])])
# Now retrieve the polygon
rs = session.execute('SELECT * FROM places')
rs.each do |row|
  puts "Layout of #{row['name']}:"
  # Write out the exterior ring
  puts "Exterior: #{row['layout'].exterior_ring}"
  # Write out the first point in the first interior ring...because we can.
  puts "First interior point: #{row['layout'].interior_rings.first.points.first}"
  # Finally, let's emit the WKT representation.
  puts "WKT: #{row['layout'].wkt}"
end
