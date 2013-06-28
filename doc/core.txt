/**

@page page-core Network Core

The Network core is built around the socket handling and brings a client and server connection
together. 

@see network_socket
@see network_mysqld_con

@section section-lifecycle Connection Life Cycle

Connections can be in one of several states which are basicly resembling the 4 basic phases
of the @ref protocol :

@li connect
@li authentification
@li query
@li disconnect

The plugins can change the default behaviour of the network core and impliment one of three 
basic plugins:

@li @ref page-plugin-admin implements only the listening side
@li client plugins implement only the connection side
@li @ref page-plugin-proxy implements both sides 

@subsection section-scripting Scripting

Most plugins implement a set of those callbacks and expose them to a scripting layer. 

For now the scriping is provided by Lua, a simple, fast and easy to embed scripting language.
We expose most of the internals into the scripting layer and provide modules to operate
on the data that we pass into the scripting layer

@section section-network-core-layer Network Core Layer

  The MySQL Proxy network engine is meant to handle several thousands connections at the same time. We 
want to use it for load-balancing and fail-over which means we have to handle the connections for
a larger group of MySQL backend servers nicely. We aim for 5k to 10k connections.
  Up to MySQL Proxy 0.7 we use a pure event-driven, non-blocking networking approach is described in
http://kegel.com/c10k.html#nb using libevent 1.4.x. 
  A event-driven design has a very small foot-print for idling connections: we just store the
connection state and let it wait for a event. 

@section section-threaded-scripting Threaded Scripting

Usually the scripts are small and only make simple decisions leaving most of the work to the network layer.
In 0.9 we will make the scripting layer multi-threaded allow several scripting threads at the same time,
working from a small pool threads.

That will allow the scripting layer to call blocking or slow functions without infecting the execution of
other connections.

Lifting the global plugin mutex will mean we have to handle access to global structure differently. Most 
of the access is happening on connection-level (the way we do the event-threading) and only access to
global structures like "proxy.global.*" has to synchronized. For that we will look into using Lua lanes
to send data around between independent Lua-states.




*/
