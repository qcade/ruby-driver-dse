# encoding: utf-8


require File.dirname(__FILE__) + '/../integration_test_case.rb'

class LdapTest < IntegrationTestCase
  def self.before_suite
    super
    @@username, @@password = @@ccm_cluster.enable_ldap
  end

  def self.after_suite
    @@ccm_cluster && @@ccm_cluster.disable_ldap
    super
  end

  # Test for basic successful ldap authentication
  #
  # test_can_authenticate_via_ldap tests basic ldap authentication to a Dse cluster using DseAuthenticator. It inputs a
  # valid username and password combination and verifies that a cluster object is created. It then performs a basic
  # query to verify that the user is indeed authenticated to the DSE cluster.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-216
  # @expected_result Username and password should successfully authenticate to the Dse cluster.
  #
  # @test_assumptions Authentication-enabled Dse cluster via LDAP.
  # @test_category dse:auth
  #
  def test_can_authenticate_via_ldap
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

  # Test for basic unsuccessful ldap authentication
  #
  # test_raise_error_on_invalid_ldap_auth tests basic ldap authentication to a Dse cluster using DseAuthenticator. It
  # inputs an empty username and password and verifies that an ArgumentError is raised. It then inputs an invalid
  # username and password combination and verifies that an AuthenticationError is raised.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-216
  # @expected_result Username and password should not successfully authenticate to the Dse cluster.
  #
  # @test_assumptions Authentication-enabled Dse cluster via LDAP.
  # @test_category dse:auth
  #
  def test_raise_error_on_invalid_ldap_auth
    assert_raises(ArgumentError) do
      Dse.cluster(username: '',
                  password: ''
      )
    end

    assert_raises(Cassandra::Errors::AuthenticationError) do
      Dse.cluster(username: 'invalidname',
                  password: 'badpassword'
      )
    end
  end
end
