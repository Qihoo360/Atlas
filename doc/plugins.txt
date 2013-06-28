/**

@page page-plugins Plugin and Scripting Layer

What is usually being referred to as <em>MySQL Proxy</em> is in fact the @link page-plugin-proxy Proxy plugin@endlink.

While the @ref page-chassis and @ref page-core make up an important part, it is really the plugins that make MySQL Proxy so flexible.

One can describe the currently available plugins as <em>connection lifecycle interceptors</em> which can register callbacks for
all states in the @ref protocol.

Currently available plugins in the main distribution include:
- @subpage page-plugin-proxy
- @subpage page-plugin-admin
- Replicator plugin
- Debug plugin
- CLI (command line) plugin

@note The latter three are not documented in-depth, mainly because they are Proof Of Concept implementations that are not targeted
for the MySQL Proxy 1.0 GA release.

*/
