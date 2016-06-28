## Kerberos Authentication
To enable kerberos authentication with DSE nodes, set the `auth_provider` of the cluster to
a `Dse::Auth::Providers::GssApi` instance. The following example code shows all the ways to set this up.
This example is also available in the examples directory.

```ruby
require 'dse'

# Create a provider for the 'dse' service and have it use the first ticket in the ticket cache for
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

# However you configure the provider, pass it to Dse.cluster to have it be used for authentication.
cluster = Dse.cluster(auth_provider: provider)
```