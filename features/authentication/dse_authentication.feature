@dse_auth
@dse-version-specific @dse-version-5.0
Feature: DSE Authentication

  DSE 5.0 introduced a DSE Unified Authenticator. The DSE Authenticator can be used for internal Cassandra plaintext
  authentication by configuring the cluster to use a given username/password for authentication to the DSE cluster.

  Background:
    Given a running dse cluster with dse authentication enabled
    And the following example:
      """ruby
      require 'dse'

      begin
        cluster = Dse.cluster(
                    username: ENV['USERNAME'],
                    password: ENV['PASSWORD']
                  )
        puts "authentication successful"
      rescue Cassandra::Errors::AuthenticationError => e
        puts "#{e.class.name}: #{e.message}"
        puts "authentication failed"
      else
        cluster.close
      end
      """

  Scenario: Authenticating with correct credentials
    When it is executed with a valid username and password in the environment
    Then its output should contain:
      """
      authentication successful
      """

  Scenario: Authenticating with incorrect credentials
    When it is executed with an invalid username and password in the environment
    Then its output should contain:
      """
      authentication failed
      """

