# This is temporary while we're getting the Ruby driver from GitHub.
require 'bundler'
Bundler.setup

require 'dse'

cluster = Dse.cluster
session = cluster.connect
result = session.execute_graph('g.V()', graph_options: {graph_name: 'mygraph'})

# Emit JSON output of each result.
puts "Number of results: #{result.size}"
result.each do |r|
  puts JSON.pretty_generate(JSON.parse(r['gremlin']))
end
