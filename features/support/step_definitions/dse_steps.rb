# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

Given(/^a running dse cluster$/) do
  step 'a running dse cluster in 1 datacenter with 3 nodes in each'
end

Given(/^a running dse cluster with dse authentication enabled$/) do
  step 'a running dse cluster'
  unless $dse_auth_enabled
    @username, @password = $cluster.enable_dse_authentication
    $dse_auth_enabled = true
  end
end

Given(/^a running dse cluster with ldap authentication enabled$/) do
  step 'a running dse cluster'
  unless $ldap_enabled
    @username, @password = $cluster.enable_ldap
    $ldap_enabled = true
  end
end

Given(/^a running dse cluster with kerberos authentication enabled$/) do
  step 'a running dse cluster'
  unless $kerberos_enabled
    @username, @password = $cluster.enable_kerberos
    $kerberos_enabled = true
  end
end

Given(/^a running dse cluster with graph enabled$/) do
  step 'a running dse cluster in 1 datacenter with 3 nodes in each with graph'
end

Given(/^a running dse cluster with graph and spark enabled$/) do
  step 'a running dse cluster in 1 datacenter with 3 nodes in each with graph and spark'
end

Given(/^a running dse cluster in (\d+) datacenter(?:s)? with (\d+) nodes in each with graph$/) do |no_dc, no_nodes_per_dc|
  $cluster = CCM.setup_graph_cluster(no_dc.to_i, no_nodes_per_dc.to_i)
end

Given(/^a running dse cluster in (\d+) datacenter(?:s)? with (\d+) nodes in each with graph and spark$/) do |no_dc, no_nodes_per_dc|
  $cluster = CCM.setup_spark_cluster(no_dc.to_i, no_nodes_per_dc.to_i)
end

Given(/^a running dse cluster in (\d+) datacenter(?:s)? with (\d+) nodes in each$/) do |no_dc, no_nodes_per_dc|
  $cluster = CCM.setup_cluster(no_dc.to_i, no_nodes_per_dc.to_i)
end

Given(/^a running dse cluster with schema:$/) do |schema|
  step 'a running dse cluster'
  step 'the following schema:', schema
end

Given(/^an existing graph called "(.*?)" with schema:$/) do |graph_name, schema|
  step "an existing graph called \"#{graph_name}\""
  step "the following graph schema for \"#{graph_name}\":", schema
end

Given(/^an existing graph called "(.*?)"$/) do |graph_name|
  replication_config = "{'class' : 'SimpleStrategy', 'replication_factor' : 3}"
  $cluster.execute_graph("system.graph('#{graph_name}').option('graph.replication_config').set(\"#{replication_config}\").ifNotExists().create()")
  $cluster.execute_graph("schema.config().option('graph.schema_mode').set(com.datastax.bdp.graph.api.model.Schema.Mode.Production)", graph_name)
  $cluster.execute_graph("schema.config().option('graph.allow_scan').set('true')", graph_name)
end

Given(/^the following graph schema for "(.*?)":$/) do |graph_name, schema|
  $cluster.setup_graph_schema(schema, graph_name)
end

