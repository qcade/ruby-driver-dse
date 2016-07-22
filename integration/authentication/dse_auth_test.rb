# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

require File.dirname(__FILE__) + '/../integration_test_case.rb'

class DseAuthTest < IntegrationTestCase
  def self.before_suite
    unless CCM.dse_version < '5.0.0'
      super
      @@username, @@password = @@ccm_cluster.enable_dse_authentication
    end
  end

  def self.after_suite
    unless CCM.dse_version < '5.0.0'
      @@ccm_cluster && @@ccm_cluster.disable_dse_authentication
      super
    end
  end

  # Test for basic successful authentication
  #
  # test_can_authenticate_via_dse_authenticator tests basic username and password authentication to a Dse
  # cluster using DseAuthenticator. It inputs a valid username and password combination and verifies that a cluster
  # object is created.
  #
  # @param username [String] The username for cluster authentication.
  # @param password [String] The password for cluster authentication.
  # @return [Dse::Cluster] The authenticated DSE cluster
  #
  # @since 1.0.0
  # @jira_ticket RUBY-169
  # @expected_result Username and password should successfully authenticate to the Dse cluster.
  #
  # @test_assumptions Authentication-enabled Dse cluster via DseAuthenticator.
  # @test_category authentication
  #
  def test_can_authenticate_via_dse_authenticator
    skip('DseAuthenticator is only available in DSE after 5.0') if CCM.dse_version < '5.0.0'

    begin
      cluster = Dse.cluster(
        username: @@username,
        password: @@password
      )
      refute_nil cluster

      session = cluster.connect
      results = session.execute('select count(*) from system.local')
      assert_equal 1, results.first['count']
    ensure
      cluster.close
    end
  end
end
