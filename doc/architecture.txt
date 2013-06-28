/**
@mainpage

The MySQL Proxy is a simple program which sits between a mysql client and a mysql server and
can inspect, transform and act on the data sent through it.

You can use it for:
@li load balancing
@li fail over
@li query tracking
@li query analysis
@li ... and much more

Internally the MySQL Proxy is a stack of:

@dotfile architecture-overview.dot

It is based on a @subpage page-core that exposes the phases of the
@subpage protocol to a @ref page-plugins.

@dot
digraph {
connect -> auth;
auth -> command;
command -> disconnect;
command -> command;
connect -> disconnect;
auth -> disconnect;
}

@enddot

Each of the phases of the life-cycle lead to several more protocol-states. For example the auth phase is made up of at least 3 packets:

@msc
	Client, Proxy, Server;

	Client -> Proxy [ label = "accept()" ];
	Proxy -> Proxy [ label = "script: connect_server()" ];
	Proxy -> Server [ label = "connect()" ];
	...;
	Server -> Proxy [ label = "recv(auth-challenge)" ];
	Proxy -> Proxy [ label = "script: read_handshake()" ];
	Proxy -> Client [ label = "send(auth-challenge)" ];
	Client -> Proxy [ label = "recv(auth-response)" ];
	Proxy -> Proxy [ label = "script: read_auth()" ];
	Server -> Proxy [ label = "send(auth-response)" ];
	Server -> Proxy [ label = "recv(auth-result)" ];
	Proxy -> Proxy [ label = "script: read_auth_result()" ];
	Proxy -> Client [ label = "send(auth-result)" ];
	...;
	
@endmsc

While the @ref page-core is scalable to a larger number of connections, the plugin/scripting
layer hides the complexity from the end-users and simplifies the customization. 

@section section-stack-of-libs Chassis, libraries and Plugins

It is built as a stack of libraries:

The @subpage page-chassis provides the common functions that all commandline and daemon applications
need: 
@li commandline and configfiles
@li logging
@li daemon/service support
@li plugin loading

The MySQL Procotol libraries which can encode and decode:
@li client protocol
@li binlog protocol
@li myisam files
@li frm files
@li masterinfo files

The @ref page-core and the @subpage page-plugins.

@dotfile architecture.dot

*/ 
