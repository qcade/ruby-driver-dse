master

Features:
* Added Kerberos support for JRuby.
* Add support for non-default kerberos caches.
* Graph queries should default to having no timeout.
* When a graph query is executed with a timeout, send the timeout to the graph server to scope execution time server-side.
* Support clearing individual options in a Dse::Graph::Options object.
* Allow "expert" options to be set in graph options.
* Support Duration datatype in graph queries.

Bug Fixes:
* [RUBY-249](https://datastax-oss.atlassian.net/browse/RUBY-249) Dse::Graph::Options.inspect erroneously reports nil option values
* [RUBY-252](https://datastax-oss.atlassian.net/browse/RUBY-252) Graph option timeout ignored when set at cluster level

1.0.0 rc2

Bug Fixes:
* Updated license info in source files
* Updated gemspec to declare dependency on v3.0.2 of core driver
* Fixed defect in build process where JRuby gem was created with the same filename as mri gem.

1.0.0 rc1

Features:
* Added Dse module with Cluster and Session classes for executing graph queries
* Added DseAuthenticator authentication-type support
* Added Kerberos authentication support
* Added geospatial type support
