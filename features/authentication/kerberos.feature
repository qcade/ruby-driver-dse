@kerberos_auth
Feature: Kerberos Authentication

  DSE 5.0 introduced a DSE Unified Authenticator. The DSE Authenticator can be used for Kerberos authentication by
  creating a GssApi provider and configuring the cluster to use the GssApi provider as the auth_provider. DSEs earlier
  than 5.0 are configured similarly but use KerberosAuthenticator on the DSE cluster.

  Background:
    Given a running dse cluster with kerberos authentication enabled
    And the following example:
      """ruby
      require 'dse'

      begin
        provider = Dse::Auth::Providers::GssApi.new(ENV['SERVICE'], true, ENV['PRINCIPAL'], ENV['TICKET_CACHE'])
        cluster  = Dse.cluster(auth_provider: provider)

        puts 'authentication successful'
      rescue Cassandra::Errors::AuthenticationError, Cassandra::Errors::NoHostsAvailable => e
        puts "#{e.class.name}: #{e.message}"
        puts 'authentication failed'
      else
        cluster.close
      end
      """

  Scenario: Authenticating with valid credentials
    And it is executed with a valid kerberos configuration in the environment
    Then its output should contain:
      """
      authentication successful
      """

  Scenario: Authenticating with an invalid service provider
    When it is executed with an invalid service provider in the environment
    Then its output should match:
      """
      Server .* not found in Kerberos database.*
      authentication failed
      """

  Scenario: Authenticating with an invalid principal
    When it is executed with an invalid principal in the environment
    Then its output should match:
      """
      (Can't find client principal baduser@DATASTAX.COM in cache collection|Unable to obtain password from user)
      authentication failed
      """

  Scenario: Authenticating with an unauthorized principal
    When it is executed with an unauthorized principal in the environment
    Then its output should match:
      """
      (dseuser@DATASTAX.COM is not permitted to log in|User dseuser@DATASTAX.COM doesn't exist - create it with CREATE USER query first)
      authentication failed
      """

  Scenario: Authenticating with a non-existent cache
    When it is executed with an invalid cache in the environment
    Then its output should match:
      """
      (No credentials cache found|Unable to obtain password from user)
      authentication failed
      """

