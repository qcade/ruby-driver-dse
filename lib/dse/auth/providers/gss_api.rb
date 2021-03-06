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
      # Auth provider to authenticate with Kerberos. Whenever the client connects to a DSE node,
      # this provider will perform Kerberos authentication operations with it. By default, the provider
      # takes the ip address of the node and uses `Socket#getnameinfo` to find its name in order to construct
      # the full service address (e.g. service@host).
      #
      # @see #initialize
      class GssApi < Cassandra::Auth::Provider
        # @private
        class NameInfoResolver
          def resolve(host)
            Socket.getnameinfo(['AF_INET', 0, host])[0]
          end
        end

        # @private
        class NoOpResolver
          def resolve(host)
            host
          end
        end

        # @private
        class Authenticator
          # Copied from kerberosgss.h
          AUTH_GSS_COMPLETE = 1

          def initialize(authentication_class, host, service, principal, ticket_cache)
            @authentication_class = authentication_class
            @host = host
            @service = service
            @principal = principal
            @ticket_cache = ticket_cache

            if RUBY_ENGINE == 'jruby'
              @sasl_client = javax.security.sasl.Sasl.createSaslClient(['GSSAPI'],
                                                                       nil,
                                                                       service,
                                                                       host,
                                                                       {javax.security.sasl.Sasl::SERVER_AUTH => 'true',
                                                                        javax.security.sasl.Sasl::QOP => 'auth'},
                                                                       nil)
              config = Dse::Auth::Providers::ChallengeEvaluator.make_configuration(principal, ticket_cache)
              login = javax.security.auth.login.LoginContext.new('DseClient', nil, nil, config)
              login.login
              @subject = login.getSubject
            else
              @gss_context = GssApiContext.new("#{@service}@#{@host}", @principal, @ticket_cache)
            end
          rescue => e
            raise Cassandra::Errors::AuthenticationError.new(
              "Failed to authenticate: #{e.message}",
              nil,
              nil,
              nil,
              nil,
              nil,
              nil,
              :one,
              0
            )
          end

          def initial_response
            @authentication_class == 'com.datastax.bdp.cassandra.auth.DseAuthenticator' ?
                'GSSAPI' :
                challenge_response('GSSAPI-START')
          end

          if RUBY_ENGINE == 'jruby'
            def challenge_response(token)
              if token == 'GSSAPI-START'
                return '' unless @sasl_client.hasInitialResponse
                token = ''
              end

              Dse::Auth::Providers::ChallengeEvaluator.evaluate(@sasl_client, @subject, token)
            end
          else
            def challenge_response(token)
              if token == 'GSSAPI-START'
                response = @gss_context.step('')[1]
              elsif !@is_gss_complete
                # Process the challenge as a next step in authentication until we have gotten
                # AUTH_GSS_COMPLETE.
                rc, response = @gss_context.step(token)
                @is_gss_complete = true if rc == AUTH_GSS_COMPLETE
                response ||= ''
              else
                # Ok, we went through all initial phases of auth and now the server is giving us a message
                # to decode.
                data = @gss_context.unwrap(token)

                raise 'Bad response from server' if data.length != 4
                parsed = data.unpack('>L').first
                max_length = [parsed & 0xffffff, 65536].min

                # Set up a response like this:
                # byte 0: the selected qop. 1==auth
                # byte 1-3: the max length for any buffer sent back and forth on this connection. (big endian)
                # the rest of the buffer: the authorization user name in UTF-8 - not null terminated.

                user_name = @gss_context.user_name
                out = [max_length | 1 << 24].pack('>L') + user_name
                response = @gss_context.wrap(out)
              end
              response
            end
          end

          def authentication_successful(token)
          end
        end

        # @param service [String] name of the kerberos service; defaults to 'dse'.
        # @param host_resolver [Boolean, Object] whether to use a host-resolver. By default,
        #        `Socket#getnameinfo` is used. To disable host-resolution, specify a `false` value. You may also
        #        provide a custom resolver, which is an object that implements the `resolve(host_ip)` method.
        # @param principal [String] The principal whose cached credentials are used to authenticate. Defaults
        #        to the first principal stored in the ticket cache.
        # @param ticket_cache [String] The ticket cache containing the cached credential we seek. Defaults
        #        *on Linux* to /tmp/krb5cc_&lt;uid&gt; (where uid is the numeric uid of the user running the
        #        client program). In MRI only, the `KRB5CCNAME` environment variable supercedes this. On Mac,
        #        the default is a symbolic reference to a ticket-cache server process.
        def initialize(service = 'dse', host_resolver = true, principal = nil, ticket_cache = nil)
          @service = service
          @host_resolver = case host_resolver
                           when false
                             NoOpResolver.new
                           when true
                             NameInfoResolver.new
                           else
                             host_resolver
                           end
          Cassandra::Util.assert_responds_to(:resolve, @host_resolver,
                                             'invalid host_resolver: it must have the :resolve method')
          @principal = principal
          @ticket_cache = ticket_cache
        end

        # @private
        def create_authenticator(authentication_class, host)
          Authenticator.new(authentication_class, @host_resolver.resolve(host.ip.to_s),
                            @service, @principal, @ticket_cache)
        end
      end
    end
  end
end
