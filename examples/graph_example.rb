# This is temporary while we're getting the Ruby driver from GitHub.
require 'bundler'
Bundler.setup

require 'dse'

def emit_result(result)
  # Emit JSON output of each result.
  puts "Number of results: #{result.size}"
  result.each do |r|
    p r
    puts r.value if r.is_a? Dse::Graph::Result
  end
end

# Connect to the cluster and get a session.
cluster = Dse.cluster
session = cluster.connect

# Default to querying the STUDIO_TUTORIAL_GRAPH graph.
session.default_graph_options.graph_name = 'STUDIO_TUTORIAL_GRAPH'

# Run a simple query to get all of the vertices in our graph (mygraph). Then print out the result.
emit_result(session.execute_graph('g.V()'))

# Use a parameterized query to limit the result size. Be careful not to use reserved words for parameter
# names (e.g. max)
emit_result(session.execute_graph('g.V().limit(my_limit)',
                                  arguments: {my_limit: 3}))

# Run a query whose result is a simple value.
emit_result(session.execute_graph('g.V().count()'))

# Run a query against a different graph.
emit_result(session.execute_graph('m.E().limit(1)', graph_options: {graph_alias: 'm'}))

# Or use a Graph Options object.
options = Dse::Graph::Options.new
options.graph_alias = 'm'
emit_result(session.execute_graph('m.E().limit(1)', graph_options: options))
