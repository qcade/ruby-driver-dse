# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
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

          def initialize(authentication_class, host, service, principal)
            @authentication_class = authentication_class
            @host = host
            @service = service
            @principal = principal

            @gss_context = GssApiContext.new("#{@service}@#{@host}", @principal)
          end

          def initial_response
            @authentication_class == 'com.datastax.bdp.cassandra.auth.DseAuthenticator' ?
                'GSSAPI' :
                challenge_response('GSSAPI-START')
          end

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

          def authentication_successful(token)
          end
        end

        # @param service [String] name of the kerberos service; typically 'dse'.
        # @param host_resolver [Boolean, Object] (true) whether to use a host-resolver. By default,
        #        `Socket#getnameinfo` is used. To disable host-resolution, specify a `false` value. You may also
        #        provide a custom resolver, which is an object that implements the `resolve(host_ip)` method.
        # @param principal [String] (nil) The principal whose cached credentials are used to authenticate. Defaults
        #        to the first principal stored in the ticket cache.
        def initialize(service, host_resolver = true, principal = nil)
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
        end

        # @private
        def create_authenticator(authentication_class, host)
          Authenticator.new(authentication_class, @host_resolver.resolve(host.ip.to_s), @service, @principal)
        end
      end
    end
  end
end
