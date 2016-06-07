# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#++

require File.dirname(__FILE__) + '/../integration_test_case.rb'

class AuthenticationTest < IntegrationTestCase
  def self.before_suite
    unless CCM.dse_version < '5.0.0'
      super
      @@username, @@password = @@ccm_cluster.enable_dse_authentication
    end
  end

  def self.after_suite
    @@ccm_cluster && @@ccm_cluster.disable_dse_authentication
    super
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
    ensure
      cluster.close
    end
  end
end
