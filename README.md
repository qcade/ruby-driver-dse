# Ruby Driver for DataStax Enterprise

This driver exposes the following features of DSE 5.0:

* <a href="#graph">Graph</a>
* <a href="#kerberos-authentication">Kerberos authentication</a> with nodes
* <a href="#geospatial-types">Geospatial types</a>

The driver depends heavily on a particular version of the core [Cassandra driver](https://rubygems.org/gems/cassandra-driver).
Within a script or irb, you can determine the exact versions of the dse and core drivers by accessing the VERSION
constant of the appropriate module:

```ruby
require 'dse'

puts "Dse Driver Version: #{Dse::VERSION}"
puts "Cassandra Driver Version: #{Cassandra::VERSION}"
```

## Graph
Executing graph statements is similar to issuing CQL queries in the Cassandra
driver. The difference is that while the Cassandra driver returns rows of results from
tables, the DSE driver returns graph result sets, which may contain domain object
representations of graph objects.

Any script using the DSE driver to execute graph queries will begin like this: 

```ruby
require 'dse'

# Connect to DSE and create a session whose graph queries will be tied to the graph
# named 'mygraph' by default. See the documentation for Dse::Graph::Options for all
# supported graph options.
cluster = Dse.cluster(graph_name: 'mygraph')
session = cluster.connect
```

The DSE driver is a wrapper around the core Cassandra driver, so any valid options to
the core driver are valid in the DSE driver as well.
 
To execute system query statements (to create a graph for example), *do not* specify a
graph name to bind to when connecting. This is illegal in DSE graph. 

### Vertices ###
Vertices in DSE Graph have properties. A property may have multiple values. This is
represented as an array when manipulating a Vertex object. A property value may also
have properties of their own (known as meta-properties). These meta-properties are
simple key-value pairs of strings; they do not nest.

```ruby
# Run a query to get all the vertices in our graph.
results = session.execute_graph('g.V()')

# Each result is a Dse::Graph::Vertex.
# Print out the label and a few of its properties.
puts "Number of vertex results: #{results.size}"
results.each do |v|
   # Start with the label
   puts "#{v.label}:"
   
   # Vertex properties support multiple values as well as meta-properties
   # (simple key-value attributes that apply to a given property's value).
   #
   # Emit the 'name' property's first value.
   puts "  name: #{v.properties['name'][0].value}"
   
   # Name again, using our abbreviated syntax
   puts "  name: #{v['name'][0].value}"
   
   # Print all the values of the 'name' property
   values = v['name'].map do |vertex_prop|
     vertex_prop.value
   end
   puts "  all names: #{values.join(',')}"
   
   # That's a little inconvenient. So use the 'values' shortcut:
   puts "  all names: #{v['name'].values.join(',')}"
   
   # Let's get the 'title' meta-property of 'name's first value.
   puts "  title: #{v['name'][0].properties['title']}"
   
   # This has a short-cut syntax as well:
   puts "  title: #{v['name'][0]['title']}"
end
```

### Edges ###
Edges connect a pair of vertices in DSE Graph. They also have properties,
but they are simple key-value pairs of strings.

```ruby
results = session.execute_graph('g.E()')

puts "Number of edge results: #{results.size}"
# Each result is a Dse::Graph::Edge object.
results.each do |e|
   # Start with the label
   puts "#{e.label}:"
   
   # Now the id's of the two vertices that this edge connects.
   puts "  in id: #{e.in_v}"
   puts "  out id: #{e.out_v}"
   
   # Edge properties are simple key-value pairs; sort of like
   # meta-properties on vertices.

   puts "  edge_prop1: #{e.properties['edge_prop1']}"
   
   # This supports the short-cut syntax as well:
   puts "  edge_prop1: #{e['edge_prop1']}"
end
```

### Path and Arbitrary Objects ###
Paths describe a path between two vertices. The graph response from DSE does not
indicate that the response is a path, so the driver cannot automatically
coerce such results into Path objects. The driver returns a DSE::Graph::Result
object in such cases, and you can coerce the result.

```ruby
results = session.execute_graph('g.V().in().path()')
puts "Number of path results: #{results.size}"
results.each do |r|
  # The 'value' of the result is a hash representation of the JSON result.
  puts "first label: #{r.value['labels'].first}"
  
  # Since we know this is a Path result, coerce it and use the Path object's methods.
  p = r.as_path
  puts "first label: #{p.labels.first}"
end
```

When a query has a simple result, the :value attribute of the result object
contains the simple value rather than a hash.

```ruby
results = session.execute_graph('g.V().count()')
puts "Number of vertices: #{results.first.value}"
```

### Miscellaneous Features ###
There are a number of other features in the api to make development easier.

```ruby
# We can access particular items in the result-set via array dereference
p results[1]

# Run a query against a different graph, but don't mess with the cluster default.
results = session.execute_graph('g.V().count()', graph_name: 'my_other__graph')

# Create a Graph Options object that we can save off and use. The graph_options arg to execute_graph
# supports an Options object.
options = Dse::Graph::Options.new
options.graph_name = 'mygraph'
results = session.execute_graph('g.V().count()', graph_options: options)

# Change the graph options on the cluster to alter subsequent query behavior.
# Switch to the analytics source in this case.
cluster.graph_options.graph_source = 'a'
results = session.execute_graph('g.V().count()')

# Create a statement object encapsulating a graph query, options, parameters,
# for ease of reuse.
statement = Dse::Graph::Statement.new('g.V().limit(n)', {n: 3}, graph_name: 'mygraph')
results = session.execute_graph(statement)
```

## Kerberos Authentication
To enable kerberos authentication with DSE nodes, set the `auth_provider` of the cluster to
a `Dse::Auth::Providers::GssApi` instance. The following example code shows all the ways to set this up.
This example is also available in the examples directory.

```ruby
require 'dse'

# Create a provider for the 'dse' service and have it use the first ticket in the ticket cache for
# authentication with nodes, which have hostname entries in the Kerberos server. All of the
# assignments below are equivalent:
provider = Dse::Auth::Providers::GssApi.new('dse')
provider = Dse::Auth::Providers::GssApi.new('dse', true)
provider = Dse::Auth::Providers::GssApi.new('dse', true, nil)

# Same as above, but this time turn off hostname resolution because the host
# info in the Kerberos server has ip's, not hostnames.
provider = Dse::Auth::Providers::GssApi.new('dse', false)

# Use a custom hostname resolver.
class MyResolver
  def resolve(ip)
    "host-#{ip}"
  end
end
provider = Dse::Auth::Providers::GssApi.new('dse', MyResolver.new)

# Specify different principal to use for authentication. This principal must already have a valid
# ticket in the Kerberos ticket cache. Also, the principal name is case-sensitive, so make sure it
# *exactly* matches your Kerberos ticket.
provider = Dse::Auth::Providers::GssApi.new('dse', true, 'cassandra@DATASTAX.COM')

# However you configure the provider, pass it to Dse.cluster to have it be used for authentication.
cluster = Dse.cluster(auth_provider: provider)
```

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
