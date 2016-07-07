# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

require File.dirname(__FILE__) + '/../integration_test_case.rb'

class KerberosTest < IntegrationTestCase
  def self.before_suite
    super
    @@ccm_cluster.enable_kerberos
  end

  def self.after_suite
    @@ccm_cluster && @@ccm_cluster.disable_kerberos
    super
  end

  # Test for basic successful kerberos authentication
  #
  # test_can_authenticate_via_kerberos tests basic kerberos authentication to a Dse cluster using DseAuthenticator. It
  # first creates a valid GssApi provider given a valid service and provider. It then connects to the cluster using
  # this provider and verifies that a cluster object is created. Finally, a simple query is executed to further
  # verify authentication was successful. It then performs these same steps using the default principal from the
  # latest cache value.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-198
  # @expected_result GssApi provider should successfully authenticate to the Dse cluster.
  #
  # @test_assumptions Authentication-enabled Dse cluster via kerberos.
  # @test_category dse:auth
  #
  def test_can_authenticate_via_kerberos

    # Principal explicitly defined
    provider = Dse::Auth::Providers::GssApi.new('dse', true, 'cassandra@DATASTAX.COM', 'cassandra.cache')
    cluster = Dse.cluster(auth_provider: provider)
    session = cluster.connect

    refute_nil cluster
    results = session.execute('select count(*) from system.local')
    assert_equal 1, results.first['count']

    # Default principal from cache
    provider = Dse::Auth::Providers::GssApi.new('dse', true, nil, 'cassandra.cache')
    cluster = Dse.cluster(auth_provider: provider)
    session = cluster.connect

    refute_nil cluster
    results = session.execute('select count(*) from system.local')
    assert_equal 1, results.first['count']
  ensure
    cluster && cluster.close
  end

  # Test for basic unsuccessful kerberos authentication
  #
  # test_raise_error_on_invalid_kerberos_auth tests basic kerberos authentication to a Dse cluster using invalid
  # credentials. It first attempts to connect without any provider and verifies that an AuthenticationError is raised.
  # It then attempts to connect once more using an invalid service name, and verifies that a NoHostsAvailable is raised.
  # It then attempts to connect without using hostname resolution, and verifies that a NoHostsAvailable error is raised.
  # It then attempts to connect using an invalid principal and verifies once more that a NoHostsAvailable is raised.
  # Finally it connects with a proper principal, but one which is unauthorized, and verifies that a AuthenticationError
  # is raised.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-198
  # @expected_result GssApi provider should not successfully authenticate to the Dse cluster.
  #
  # @test_assumptions Authentication-enabled Dse cluster via kerberos.
  # @test_category dse:auth
  #
  def test_raise_error_on_invalid_kerberos_auth
    # No provider specified
    assert_raises(Cassandra::Errors::AuthenticationError) do
      Dse.cluster
    end

    # Invalid service
    provider = Dse::Auth::Providers::GssApi.new('badprovider', true, 'cassandra@DATASTAX.COM', 'cassandra.cache')
    assert_raises(Cassandra::Errors::NoHostsAvailable) do
      Dse.cluster(auth_provider: provider)
    end

    # # No host resolution
    # provider = Dse::Auth::Providers::GssApi.new('dse', false, 'cassandra@DATASTAX.COM', 'cassandra.cache')
    # assert_raises(Cassandra::Errors::NoHostsAvailable) do
    #   Dse.cluster(auth_provider: provider)
    # end

    # Invalid principal
    provider = Dse::Auth::Providers::GssApi.new('dse', true, 'baduser@DATASTAX.COM', 'cassandra.cache')
    assert_raises(Cassandra::Errors::AuthenticationError) do
      Dse.cluster(auth_provider: provider)
    end

    # Unauthorized principal
    provider = Dse::Auth::Providers::GssApi.new('dse', true, 'dseuser@DATASTAX.COM', 'dseuser.cache')
    assert_raises(Cassandra::Errors::AuthenticationError) do
      Dse.cluster(auth_provider: provider)
    end
  end
end
