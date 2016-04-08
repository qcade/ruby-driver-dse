# Ruby Driver for DataStax Enterprise

This driver exposes the following features of DSE 5.0:

* Graph
* Geospatial types
* Kerberos authentication with nodes

## Graph
Here's a simple use of the graph api:

```ruby
require 'dse'

cluster = Dse.cluster
session = cluster.connect
result = session.execute_graph('g.V()', graph_options: {graph_name: 'mygraph'})

# Emit JSON output of each result.
puts "Number of results: #{result.size}"
result.each do |r|
  puts JSON.pretty_generate(JSON.parse(r['gremlin']))
end
```

## Geospatial Types
TODO

## Kerberos Authentication
TODO
