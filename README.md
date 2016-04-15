# Ruby Driver for DataStax Enterprise

This driver exposes the following features of DSE 5.0:

* Graph
* Geospatial types
* Kerberos authentication with nodes

## Graph
Here's a run-down of the graph api:

```ruby
require 'dse'

# Connect to DSE and create a session whose graph queries will be tied to the graph
# named 'mygraph' by default. See the documentation for Dse::Graph::Options for all
# supported graph options.
cluster = Dse.cluster
session = cluster.connect(graph_name: 'mygraph')

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

# Let's do edges now. Each result is a Dse::Graph::Edge
results = session.execute_graph('g.E()')

puts "Number of edge results: #{results.size}"
results.each do |e|
   # Start with the label
   puts "#{e.label}:"
   
   # Now the id's of the two vertices that this edge connects.
   puts "  in id: #{e.inV}"
   puts "  out id: #{e.outV"
   
   # Edge properties are simple key-value pairs; sort of like
   # meta-properties on vertices.

   puts "  edge_prop1: #{e.properties['edge_prop1']}"
   
   # This supports the short-cut syntax as well:
   puts "  edge_prop1: #{e['edge_prop1']}"
end

# Other complex results end up in a Dse::Graph::Result object
results = session.execute_graph('g.V().in().path()')
puts "Number of path results: #{results.size}"
results.each do |r|
  # The 'value' of the result is a hash representation of the JSON result.
  puts "first label: #{r.value['labels'].first}"
  
  # We do have a class that wraps path JSON, but unlike Vertex and Edge, where the JSON
  # indicates the type explicitly, path results do not, so we can't automatically
  # create them within the api. So, you do it yourself.
  
  p = r.as_path
  puts "first label: #{p.labels.first}"
end

# We can also access particular items in the result-set via array dereference
p results[1]

# Handling simple results is...simple! Dse::Graph::Result's 'value' attribute is the simple value. 
results = session.execute_graph('g.V().count()')
puts "Number of vertices: #{results.first.value}"

# Run a query against a different graph, but don't mess with the session default.
results = session.execute_graph('g.V().count()', graph_options: {graph_name: 'my_other__graph'})

# Create a Graph Options object that we can save off and use. The graph_options arg to execute_graph
# supports an Options object as well as Hash.
options = Dse::Graph::Options.new
options.graph_name = 'mygraph'
results = session.execute_graph('g.V().count()', graph_options: options)

# Change the graph options on the session to alter subsequent query behavior.
session.graph_options.graph_alias = 'm'
results = session.execute_graph('m.V().count()')
```

## Geospatial Types
TODO

## Kerberos Authentication
TODO
