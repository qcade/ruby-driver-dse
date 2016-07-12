# Ruby Driver for DataStax Enterprise

*NOTE: The DataStax Enterprise Ruby Driver can be used solely with DataStax Enterprise. Please consult [the license](http://www.datastax.com/terms/datastax-dse-driver-license-terms).*

This is the documentation for the DataStax Enterprise Ruby Driver for DSE. This
driver is built on top of the [DataStax Ruby driver for Apache Cassandra](http://docs.datastax.com/en/latest-ruby-driver/ruby-driver/whatsNew.html)
and enhanced for the adaptive data management and mixed workload capabilities
provided by DSE. Therefore a lot of the underlying concepts are the same and 
to keep this documentation focused we will be linking to the relevant sections
of the DataStax Ruby driver for Apache Cassandra documentation where necessary.

Within a script or irb, you can determine the exact versions of the dse and core drivers by accessing the VERSION
constant of the appropriate module:

```ruby
require 'dse'

puts "Dse Driver Version: #{Dse::VERSION}"
puts "Cassandra Driver Version: #{Cassandra::VERSION}"
```

This driver exposes the following features of DSE 5.0:

* <a href="#graph">Graph</a>
* <a href="#authentication">Authentication</a> with nodes running DSE
* <a href="#geospatial-types">Geospatial types</a>


## Installation
The driver is named dse-driver on rubygems.org and can easily be installed with Bundler or the gem program. It will
download the appropriate Cassandra driver as well.

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

### Duration Graph Type ###
DSE Graph supports several [datatypes](
http://docs.datastax.com/en/latest-dse/datastax_enterprise/graph/reference/refDSEGraphDataTypes.html)
for properties. The *Duration* type represents a duration of time. When DSE Graph returns properties of this type,
the string representation is non-trivial and requires parsing in order for the user to really gain any information from it.

The driver includes a helper class to parse such responses from DSE graph as well as to send such values in bound
paramters in requests:

```ruby
# Create a Duration property in the schema called 'runtime' and declare that 'process' vertices can have this property.
session.execute_graph(
    "schema.propertyKey('runtime').Duration().ifNotExists().create();
      schema.propertyKey('name').Text().ifNotExists().create();
      schema.vertexLabel('process').properties('name', 'runtime').ifNotExists().create()")

# We want to record that a process ran for 1 hour, 2 minutes, 3.5 seconds.
runtime = Dse::Graph::Duration.new(0, 1, 2, 3.5)
session.execute_graph(
    "graph.addVertex(label, 'process', 'name', 'calculator', 'runtime', my_runtime);",
    arguments: {'my_runtime' => runtime})

# Now retrieve the vertex. Assume this is the only vertex in the graph for simplicity. 
v = session.execute_graph('g.V()').first
runtime = Dse::Graph::Duration.parse(v['runtime'].first.value)
puts "#{runtime.hours} hours, #{runtime.minutes} minutes, #{runtime.seconds} seconds"
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

# Set an "expert" option for which we don't have accessor methods.
# NOTE: Such options are not part of the public api and may change in a future release of DSE.
options.set('super-cool-option', true)

# Change the graph options on the cluster to alter subsequent query behavior.
# Switch to the analytics source in this case.
cluster.graph_options.graph_source = 'a'
results = session.execute_graph('g.V().count()')

# Create a statement object encapsulating a graph query, options, parameters,
# for ease of reuse.
statement = Dse::Graph::Statement.new('g.V().limit(n)', {n: 3}, graph_name: 'mygraph')
results = session.execute_graph(statement)
```

## Authentication
DSE 5.0 introduces [DSE Unified Authentication](http://docs.datastax.com/en/datastax_enterprise/5.0/datastax_enterprise/unifiedAuth/unifiedAuthConfig.html),
which supports multiple authentication schemes concurrently. Thus, different clients may authenticate with any
authentication provider that is supported under the "unified authentication" umbrella: internal authentication, LDAP,
and Kerberos.

*NOTE:* the authentication providers described below are backward-compatible with legacy authentication mechanisms
provided by older DSE releases. So, feel free to use these providers regardless of your DSE environment.

### Internal and LDAP Authentication
Just as [Cassandra::Auth::Providers::Password](http://docs.datastax.com/en/developer/ruby-driver/3.0/supplemental/api/cassandra/auth/providers/password/?local=true&nav=toc)
handles internal and LDAP authentication with Cassandra, the `Dse::Auth::Providers::Password` provider handles these types of
authentication in DSE 5.0 configured with DseAuthenticator. The Ruby DSE driver makes it very easy to authenticate with username and password:
```ruby
cluster = Dse.cluster(username: 'user', password: 'pass')
```
The driver creates the provider under the hood and configures the cluster object appropriately.

### Kerberos Authentication

#### Initial Setup
Unlike other authentication mechanisms, Kerberos requires some set-up on the client. First, set the `KRB5_CONFIG`
environment variable to the location of your `krb5.conf` file and use `kinit` to obtain a ticket from your 
Kerberos server. 

This environment variable is also needed by the Ruby DSE driver when run in an MRI Ruby interpreter.
This is due to the fact that Kerberos support is implemented as a C extension that uses the gssapi system libraries --
the same libraries that command line tools like kinit use.

The JRuby implementation of Kerberos support uses the Java security framework, which requires
the `java.security.krb5.conf` system property to be set to the location of the `krb5.conf` file. One way to
accomplish this is to set the `JRUBY_OPTS` environment variable before running your client application:

```
export JRUBY_OPTS="-J-Djava.security.krb5.conf=/home/user1/krb5.conf"
```

#### Configuring the Client
To enable kerberos authentication with DSE nodes, set the `auth_provider` of the cluster to
a `Dse::Auth::Providers::GssApi` instance. The following example code shows all the ways to set this up.
This example is also available in the examples directory.

```ruby
require 'dse'

# Create a provider for the 'dse' service and have it use the first ticket in the default ticket cache for
# authentication with nodes, which have hostname entries in the Kerberos server. All of the
# assignments below are equivalent:
provider = Dse::Auth::Providers::GssApi.new
provider = Dse::Auth::Providers::GssApi.new('dse')
provider = Dse::Auth::Providers::GssApi.new('dse', true)
provider = Dse::Auth::Providers::GssApi.new('dse', true, nil)

# Same as above, but this time turn off hostname resolution because the Kerberos server
# may be configured with ip's, not hostnames, of DSE nodes.
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

#### Ticket Caches
By default, `kinit` and related tools (e.g. `klist`, `kdestroy`) manipulate a simple file tied to the client os user's
numeric id on Linux: `/tmp/krb5cc_<uid>`. This file only supports one "ticket granting ticket", so if you have a need for
multiple credentials in your system (e.g. multiple applications each of which need to authenticate with different
credentials to different services), you can supply the `-c` argument to kinit to authenticate and store the resulting
ticket in a different cache. In that set-up, you must initialize your `auth_provider` in the driver with this info:

```ruby
# The fourth arg is the path to the cache file. 
provider = Dse::Auth::Providers::GssApi.new('dse', true, nil, '/home/myuser/krb.cache')
```

For MRI (the underlying gssapi C library, actually), you can set the `KRB5CCNAME` environment variable instead of
supplying an extra argument to the provider constructor.

Mac supports non-default caches as well, but it's not necessary because by default the default cache is an in-memory
store that supports multiple tickets.

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

## License
Copyright (C) 2016 DataStax Inc.

The full license terms are available at http://www.datastax.com/terms/datastax-dse-driver-license-terms