# This is temporary while we're getting the Ruby driver from GitHub.
require 'bundler'
Bundler.setup

require 'dse'

def emit_result(result)
  # Emit JSON output of each result.
  puts "Number of results: #{result.size} on #{result.execution_info.hosts.last.ip}"
  result.each do |r|
    p r
    puts r.value if r.is_a? Dse::Graph::Result
  end
end

# Connect to the cluster and get a session whose graph queries will be tied to the graph
# named STUDIO_TUTORIAL_GRAPH by default. See the documentation for Dse::Graph::Options for all
# supported graph options.
cluster = Dse.cluster(graph_name: 'STUDIO_TUTORIAL_GRAPH')
session = cluster.connect

puts '-- Run a simple query to get all vertices in our graph. --'
emit_result(session.execute_graph('g.V()'))

puts '-- Run a parameterized query to limit result size to 3. --'
emit_result(session.execute_graph('g.V().limit(my_limit)',
                                  arguments: {my_limit: 3}))

puts '-- Run a query whose result is a simple value. --'
emit_result(session.execute_graph('g.V().count()'))

puts '-- Run a query with the analytics graph source. --'
emit_result(session.execute_graph('g.E().limit(1)', graph_source: 'a'))

puts '-- Or use a Graph Options object. --'
options = Dse::Graph::Options.new
options.graph_source = 'a'
emit_result(session.execute_graph('g.E().limit(1)', graph_options: options))

puts '-- Change the graph source on the cluster so that all future queries use it. --'
cluster.graph_options.graph_source = 'a'
emit_result(session.execute_graph('g.E().limit(1)'))

puts '-- Use a Graph Statement object --'
statement = Dse::Graph::Statement.new('g.E().limit(1)', nil, graph_options: options)
emit_result(session.execute_graph(statement))
