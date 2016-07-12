## Authentication
DSE 5.0 introduces [DSE Unified Authentication](http://docs.datastax.com/en/datastax_enterprise/5.0/datastax_enterprise/unifiedAuth/unifiedAuthConfig.html),
which supports multiple authentication schemes concurrently. Thus, different clients may authenticate with any
authentication provider that is supported under the "unified authentication" umbrella: internal authentication, LDAP,
and Kerberos.

*NOTE:* the authentication providers described below are backward-compatible with legacy authentication mechanisms
provided by older DSE releases. So, feel free to use these providers regardless of your DSE environment.

### Internal and LDAP Authentication
Just as [Cassandra::Auth::Providers::Password](http://docs.datastax.com/en/developer/ruby-driver/3.0/supplemental/api/cassandra/auth/providers/password/?local=true&nav=toc)
handles internal and LDAP authentication with Cassandra, the `Dse::Auth::Providers::Password` provider handles these types of
authentication in DSE 5.0 configured with DseAuthenticator. The Ruby DSE driver makes it very easy to authenticate with username and password:
```ruby
cluster = Dse.cluster(username: 'user', password: 'pass')
```
The driver creates the provider under the hood and configures the cluster object appropriately.

### Kerberos Authentication

#### Initial Setup
Unlike other authentication mechanisms, Kerberos requires some set-up on the client. First, set the `KRB5_CONFIG`
environment variable to the location of your `krb5.conf` file and use `kinit` to obtain a ticket from your 
Kerberos server. 

This environment variable is also needed by the Ruby DSE driver when run in an MRI Ruby interpreter.
This is due to the fact that Kerberos support is implemented as a C extension that uses the gssapi system libraries --
the same libraries that command line tools like kinit use.

The JRuby implementation of Kerberos support uses the Java security framework, which requires
the `java.security.krb5.conf` system property to be set to the location of the `krb5.conf` file. One way to
accomplish this is to set the `JRUBY_OPTS` environment variable before running your client application:

```
export JRUBY_OPTS="-J-Djava.security.krb5.conf=/home/user1/krb5.conf"
```

#### Configuring the Client
To enable kerberos authentication with DSE nodes, set the `auth_provider` of the cluster to
a `Dse::Auth::Providers::GssApi` instance. The following example code shows all the ways to set this up.
This example is also available in the examples directory.

```ruby
require 'dse'

# Create a provider for the 'dse' service and have it use the first ticket in the default ticket cache for
# authentication with nodes, which have hostname entries in the Kerberos server. All of the
# assignments below are equivalent:
provider = Dse::Auth::Providers::GssApi.new
provider = Dse::Auth::Providers::GssApi.new('dse')
provider = Dse::Auth::Providers::GssApi.new('dse', true)
provider = Dse::Auth::Providers::GssApi.new('dse', true, nil)

# Same as above, but this time turn off hostname resolution because the Kerberos server
# may be configured with ip's, not hostnames, of DSE nodes.
provider = Dse::Auth::Providers::GssApi.new('dse', false)

# Use a custom hostname resolver.
class MyResolver
  def resolve(ip)
    "host-#{ip}"
  end
end
provider = Dse::Auth::Providers::GssApi.new('dse', MyResolver.new)

# Specify different principal to use for authentication. This principal must already have a valid
# ticket in the Kerberos ticket cache. Also, the principal name is case-sensitive, so make sure it
# *exactly* matches your Kerberos ticket.
provider = Dse::Auth::Providers::GssApi.new('dse', true, 'cassandra@DATASTAX.COM')

# However you configure the provider, pass it to Dse.cluster to have it be used for authentication.
cluster = Dse.cluster(auth_provider: provider)
```

#### Ticket Caches
By default, `kinit` and related tools (e.g. `klist`, `kdestroy`) manipulate a simple file tied to the client os user's
numeric id on Linux: `/tmp/krb5cc_<uid>`. This file only supports one "ticket granting ticket", so if you have a need for
multiple credentials in your system (e.g. multiple applications each of which need to authenticate with different
credentials to different services), you can supply the `-c` argument to kinit to authenticate and store the resulting
ticket in a different cache. In that set-up, you must initialize your `auth_provider` in the driver with this info:

```ruby
# The fourth arg is the path to the cache file. 
provider = Dse::Auth::Providers::GssApi.new('dse', true, nil, '/home/myuser/krb.cache')
```

For MRI (the underlying gssapi C library, actually), you can set the `KRB5CCNAME` environment variable instead of
supplying an extra argument to the provider constructor.

Mac supports non-default caches as well, but it's not necessary because by default the default cache is an in-memory
store that supports multiple tickets.
