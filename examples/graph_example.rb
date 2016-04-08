# This is temporary while we're getting the Ruby driver from GitHub.
require 'bundler'
Bundler.setup

require 'dse'

def emit_result(result)
  # Emit JSON output of each result.
  puts "Number of results: #{result.size}"
  result.each do |r|
    puts JSON.pretty_generate(JSON.parse(r['gremlin']))
  end
end

# Connect to the cluster and get a session.
cluster = Dse.cluster
session = cluster.connect

# Run a simple query to get all of the vertices in our graph (mygraph). Then print out the result.
emit_result(session.execute_graph('g.V()', graph_options: {graph_name: 'mygraph'}))

# Use a parameterized query to limit the result size. Be careful not to use reserved words for parameter
# names (e.g. max)
emit_result(session.execute_graph('g.V().limit(my_limit)',
                                  arguments: {my_limit: 3},
                                  graph_options: {graph_name: 'mygraph'}))
