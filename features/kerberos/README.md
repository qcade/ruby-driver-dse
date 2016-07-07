## Kerberos Authentication
To enable kerberos authentication with DSE nodes, set the `auth_provider` of the cluster to
a `Dse::Auth::Providers::GssApi` instance. The following example code shows all the ways to set this up.
This example is also available in the examples directory.

```ruby
require 'dse'

# Create a provider for the 'dse' service and have it use the first ticket in the default ticket cache for
# authentication with nodes, which have hostname entries in the Kerberos server. All of the
# assignments below are equivalent:
provider = Dse::Auth::Providers::GssApi.new('dse')
provider = Dse::Auth::Providers::GssApi.new('dse', true)
provider = Dse::Auth::Providers::GssApi.new('dse', true, nil)

# Same as above, but this time turn off hostname resolution because the host
# info in the Kerberos server has ip's, not hostnames.
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

# All of the above examples use the default ticket cache. However, you can supply a fourth argument if
# your ticket cache is not the system default. The system default depends on your os-platform and ruby-platform:
#
# Linux:
#  MRI: if the KRB5CCNAME environment variable is set, respect it. Otherwise, /tmp/krb5cc_<uid> where uid is the
#       numeric os user-id of the user.
#  JRuby: /tmp/krb5cc_<uid> where uid is the numeric os user-id of the user. If not present, fall back to
#         $HOME/krb5cc_<user-name>.
# Mac: the kerberos cache is actually a daemon. Both MRI and JRuby respect it.
provider = Dse::Auth::Providers::GssApi.new('dse', true, nil, '/home/myuser/krb.cache')

# However you configure the provider, pass it to Dse.cluster to have it be used for authentication.
cluster = Dse.cluster(auth_provider: provider)
```
