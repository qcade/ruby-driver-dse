# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

When(/^it is executed with a valid username and password in the environment$/) do
  with_environment('USERNAME' => @username, 'PASSWORD' => @password) do
    step 'it is executed'
  end
end

When(/^it is executed with an invalid username and password in the environment$/) do
  with_environment('USERNAME' => 'invalidname', 'PASSWORD' => 'badpassword') do
    step 'it is executed'
  end
end

When(/^it is executed with a valid kerberos configuration in the environment$/) do
  with_environment('SERVICE' => 'dse',
                   'PRINCIPAL' => 'cassandra@DATASTAX.COM',
                   'TICKET_CACHE' => ENV['WORKSPACE'] + '/cassandra.cache') do
    step 'it is executed'
  end
end

When(/^it is executed with an invalid service provider in the environment$/) do
  with_environment('SERVICE' => 'badprovider',
                   'PRINCIPAL' => 'cassandra@DATASTAX.COM',
                   'TICKET_CACHE' => ENV['WORKSPACE'] + '/cassandra.cache') do
    step 'it is executed'
  end
end

When(/^it is executed with an invalid principal in the environment$/) do
  with_environment('SERVICE' => 'dse',
                   'PRINCIPAL' => 'baduser@DATASTAX.COM',
                   'TICKET_CACHE' => nil) do
    step 'it is executed'
  end
end

When(/^it is executed with an unauthorized principal in the environment$/) do
  with_environment('SERVICE' => 'dse',
                   'PRINCIPAL' => 'dseuser@DATASTAX.COM',
                   'TICKET_CACHE' => ENV['WORKSPACE'] + '/dseuser.cache') do
    step 'it is executed'
  end
end

When(/^it is executed with an invalid cache in the environment$/) do
  with_environment('SERVICE' => 'dse',
                   'PRINCIPAL' => 'cassandra@DATASTAX.COM',
                   'TICKET_CACHE' => ENV['WORKSPACE'] + '/bad.cache') do
    step 'it is executed'
  end
end

When(/^it is executed with a valid ca path in the environment$/) do
  with_environment('SERVER_CERT' => @server_cert) do
    step 'it is executed'
  end
end

When(/^it is executed with ca and cert path and key in the environment$/) do
  with_environment('SERVER_CERT' => @server_cert, 'CLIENT_CERT' => @client_cert,
                   'PRIVATE_KEY' => @private_key, 'PASSPHRASE' => @passphrase) do
    step 'it is executed'
  end
end

