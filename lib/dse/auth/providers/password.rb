# encoding: utf-8

#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

module Dse
  module Auth
    module Providers
      # Auth provider to authenticate with username/password for DSE's built-in authentication as well as LDAP.
      #
      # @note No need to instantiate this class manually, use `:username` and
      #   `:password` options when calling {Dse.cluster} and one will be
      #   created automatically for you.

      class Password < Cassandra::Auth::Provider
        # @private
        class Authenticator
          def initialize(authentication_class, username, password)
            @authentication_class = authentication_class
            @username = username
            @password = password
          end

          def initial_response
            @authentication_class == 'com.datastax.bdp.cassandra.auth.DseAuthenticator' ?
                'PLAIN' :
                challenge_response('PLAIN-START')
          end

          def challenge_response(token)
            "\x00#{@username}\x00#{@password}"
          end

          def authentication_successful(token)
          end
        end

        # @param username [String] username to use for authentication to Cassandra
        # @param password [String] password to use for authentication to Cassandra
        def initialize(username, password)
          @username = username
          @password = password
        end

        # @private
        def create_authenticator(authentication_class, host)
          Authenticator.new(authentication_class, @username, @password)
        end
      end
    end
  end
end
