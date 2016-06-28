## Geospatial Types
DataStax Enterprise v5.0 adds support for three geospatial types in the underlying Cassandra 3.x database. Instances of
these types can be expressed in [well-known text (WKT)](https://en.wikipedia.org/wiki/Well-known_text) form as well as a
binary representation known as [well-known binary (WKB)](https://en.wikipedia.org/wiki/Well-known_text#Well-known_binary).
This latter representation is sent over the wire between the client and DSE node, but the former makes it easy to
submit queries with geospatial type references in cqlsh.

For example, if you had a `points` table with an int key `f1` and a PointType column `p`, you could insert a row into
it like this in cqlsh: `INSERT INTO points (f1, p) VALUES (7, 'POINT (32.0 12.0)');` You can compose points into
line-strings and you can compose line-strings into polygons. See
[this section of the WKT documentation](https://en.wikipedia.org/wiki/Well-known_text#Geometric_objects) for details.

### Point
A *Point* is a point with x,y coordinates. Columns in DSE have the custom type `org.apache.cassandra.db.marshal.PointType`.

```ruby
# The geospatial types are defined in the Dse::Geometry module. Save some typing and include it
# here so that we can refer to the classes with their base names.
include Dse::Geometry

# Create a table with a PointType column and insert a row into it.
session.execute("CREATE TABLE IF NOT EXISTS points_of_interest" \
                " (name text PRIMARY KEY, coords 'PointType')")
session.execute('INSERT INTO points_of_interest (name, coords) VALUES (?, ?)',
                arguments: ['Empire State', Point.new(38.0, 21.0)])

# Now retrieve the point.
rs = session.execute('SELECT * FROM points_of_interest')
rs.each do |row|
  # We can emit the point in its WKT representation.
  puts "#{row['name']}   #{row['coords'].wkt}"

  # Or the x and y coordinates
  puts "#{row['name']}   #{row['coords'].x},#{row['coords'].y}"

  # Which is really the to_s of the point, so you can do this:
  puts "#{row['name']}   #{row['coords']}"
end
```

### LineString
A *LineString* is a set of lines, characterized by a sequence of *Point*s. As *Point*s live in the 2D xy-plane,
so do *LineString*s. Each line shares a point with another line, thus forming a string of lines. A real-world
example of this is a path on a map. Columns in DSE have the custom type `org.apache.cassandra.db.marshal.LineStringType`.

```ruby
# The geospatial types are defined in the Dse::Geometry module. Save some typing and include it
# here so that we can refer to the classes with their base names.
include Dse::Geometry

# Create a table with a LineString column and insert a row into it.
session.execute("CREATE TABLE IF NOT EXISTS directions" \
                " (origin text PRIMARY KEY, destination text, directions 'LineStringType')")
session.execute('INSERT INTO directions (origin, destination, directions) VALUES (?, ?, ?)',
                arguments: ['office', 'home', LineString.new(Point.new(12.0, 21.0),
                                                             Point.new(13.0, 31.0),
                                                             Point.new(14.0, 41.0))])
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
```

### Polygon
A *Polygon* is an enclosed shape consisting of a set of _linear-rings_. A linear-ring is a *LineString* whose last
point is the same as its first point (thus forming a ring when you connect the points). The first ring specified in a
polygon defines the outer edges of the polygon and is called the _exterior ring_. A polygon may also have _holes_
within it, specified by other linear-rings, and those holes may contain linear-rings indicating _islands_. All
such rings are called _interior rings_.

```ruby

# The geospatial types are defined in the Dse::Geometry module. Save some typing and include it
# here so that we can refer to the classes with their base names.
include Dse::Geometry

# Create a table with a Polygon column and insert a row into it. A polygon consists of a set
# of linear-rings. A linear-ring is a LineString whose last point is the same as its first point.

session.execute("CREATE TABLE IF NOT EXISTS places (name text PRIMARY KEY, layout 'PolygonType')")
exterior_ring = LineString.new(Point.new(0, 0),
                               Point.new(20, 0),
                               Point.new(26, 26),
                               Point.new(0, 26),
                               Point.new(0, 0))
                                 
interior_ring = LineString.new(Point.new(1, 1),
                               Point.new(1, 5),
                               Point.new(5, 5),
                               Point.new(5, 1),
                               Point.new(1, 1))
session.execute('INSERT INTO places (name, layout) VALUES (?, ?)',
                arguments: ['Capitol', Polygon.new(exterior_ring, interior_ring)])

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
```
