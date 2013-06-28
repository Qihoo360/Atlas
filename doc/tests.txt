/**
@page page.test-multithreading-network-io Testing: Performance and Scalability

Prior to the multithreading changes in MySQL Proxy 0.8 performance and scalability testing was rather limited in scope, mainly because the user did not have too many options tuning it.

The rule of thumb given for estimating performance characteristics went as follows:

In a single-threaded imlpementation, overall throughput of MySQL Proxy entirely depends on the workload. The variables are:
 - Rate of connection establishment
 - Ratio of active and idling connections
 - Average and standard deviation of query execution times
 - Average and standard deviation of result set sizes
 - "Synchronicity" of network events (such as queries and result sets coming in)
 - Execution time of the Lua script callbacks

Note that none of these variables can be directly influenced by the users, maybe with the slight exception of the execution time of the Lua scripts, but even there exists a lower limit and it is usually hard to optimize those in a controlled way, since there's hardly any profiling tools available.
The rule of thumb then is that the more time MySQL Proxy spends waiting for network events, the better. This observation results from the insight that with event-driven non-blocking I/O the "time in between" events can used to make progress on other connections.<br>
However, the workload many of our targeted customers aim for is exactly the worst case for MySQL Proxy: Short execution times, small result sets, high rate of new incoming connections (think PHP), and a low rate of active to idle connections.<br>
Events arriving at roughly the same point in time (the "synchronicity" point above) usually is beyond our and the user's control so we cannot influence that. But it has a high influence on overall throughput because in a single-threaded implementation the progress on the event coming on later would stall until the prior event has been processed.

Careful measurements have shown that if you design a testing scenario in such a way that it targets the worst case, throughput could be as much as 75% worse than if connecting to the backend server directly. In practice this scenario happens rarely, but nonetheless it is unacceptable.

Since the multithreading implementation address some of the points directly (by having more workers to make progress on the independent network events) much of the drawbacks should go away. Furthermore the user now has a directly tunable parameter to adapt MySQL Proxy to his workload: The number of network threads.

The purpose of performance testing then is to quantify the impact architecture and implementation decisions have on the overall throughput, in order to verify our approach, to highlight areas where MySQL Proxy needs improvement and to prevent regressions.

The rest of this document gives an overview of the testing scenarios we plan to employ to cover the above mentioned cases. They are not primarily meant to verify that MySQL Proxy is working correctly, although it is a sideeffect.

@li @subpage variables
@li @subpage information_collected
@li @subpage tools

@li @ref section-test_idle_conn : Establish a lot of connections and let them sit idle (basic event handling testing).
@li @ref section-test_short_lived_connections : Establish a lot of short-lived connections, quickly and in succession, 
from many clients (i.e. simulating fast queries from e.g. PHP applications without a connection pooler).
@li @ref section-test_active_connections : Establish a lot of connections, mainly doing network i/o. Such as selecting huge resultsets and/or inserting blobs.
@li @ref section-test_semi_active_connections : The same as the previous test, but be active only on a subset of the connections (if you have n network threads and m connections only do work on m/n of them) - this tests the event distribution algorithm in the network i/o code (currently every event is distributed to an idle thread).
@li @ref section-test_cont_test : Run the MySQL Proxy under load as close to a production like web application environment as possible.
@li @ref section-test_ha_test : Kill different components and see how the MySQL Proxy reacts.
@li @ref section-test_performance_test : This test applies to all previous test scenarios.


@section Tests

@subsection section-test_idle_conn Idle Connections

We will use a simple php script that will stablish a connection, and stay idle (using PHP's sleep() function) 
for an specified number of minutes.

@subsection section-idle_conn Idle Connections test variables

On this test we will use different values for:

@li <b>(c)</b> Number of connections.
@li <b>(t)</b> Number of threads.
@li <b>(r)</b> Rate of connection we establish.


@subsection section-test_short_lived_connections Short lived Connections


We will resuse the scripts for @ref page.test.idle_conn and add a query like SELECT 1;

@subsection section-short_lived_conn Short lived Connections test variables

On this test we will use different values for:

@li <b>(c)</b> Number of connections.
@li <b>(t)</b> Number of threads.
@li <b>(r)</b> Rate of connection we establish.
@li <b>(a)</b> Number of active connections (i.e. connections on which there actually is something going on).


@subsection section-test_active_connections Active Connections


Here we will focus on large resultsets, BLOB/TEXT column types. As well as resultsets that bring back large number of rows.


@subsection section-active_conn Active Connections test variables

On this test we will use different values for:

@li <b>(c)</b> Number of connections.
@li <b>(t)</b> Number of threads.
@li <b>(r)</b> Rate of connection we establish.
@li <b>(a)</b> Number of active connections (i.e. connections on which there actually is something going on).
@li <b>(s_l) (size_lower)</b> Lower size limit of the resultset on each connection.
@li <b>(s_u) (size_upper)</b> Upper size limit of the resultset on each connection.

@subsection section-test_semi_active_connections Subset Active Connections


Here we will test how the MySQL Proxy distributes the load among the available threads.
We will resuse the scripts for @ref page.test.idle_conn and make the nesessary modifications,
to only have a subset of those connections as active connections.


@subsection section-semi_active_conn Subset Active Connections test variables

On this test we will use different values for:


@li <b>(c)</b> Number of connections.
@li <b>(t)</b> Number of threads.
@li <b>(r)</b> Rate of connection we establish.
@li <b>(a)</b> Number of active connections (i.e. connections on which there actually is something going on).
@li <b>(s_l) (size_lower)</b> Lower size limit of the resultset on each connection.
@li <b>(s_u) (size_upper)</b> Upper size limit of the resultset on each connection.


@subsection section-test_cont_test Continuous Test

Using jMeter and a web application (SugarCRM), drive load through the proxy and let it run for weeks at the time.
jMeter allows us to adjust the number of web users that go to the SugarCRM application, as well as collect information about response times, etc.

We will modify our current implementation of jMeter to send the <b>same</b> set of queries all the time. 
We currently use a random load which is not useful if you try to compare results.

On this test we will monitor resources used by the proxy (CPU, Memory, etc)

@subsection section-cont_test Continuous test variables

On this test we will use different values for:

@li <b>(c)</b> Number of connections.
@li <b>(t)</b> Number of threads.
@li <b>(r)</b> Rate of connection we establish.
@li <b>(a)</b> Number of active connections (i.e. connections on which there actually is something going on).


@subsection section-test_ha_test HA Test

Here we will force different failures and monitor how the MySQL Proxy handles those states.
We will:
@li Kill the MySQL Proxy (and test the keepalive option).
@li Drop network packets (using a firewall rule).
@li Kill the backend MySQL server.
@li Kill the connections right after they have sent a query through the proxy.

@subsection section-ha_test HA test variables

On this test we will use different values for:

@li <b>(c)</b> Number of connections.
@li <b>(t)</b> Number of threads.
@li <b>(r)</b> Rate of connection we establish.
@li <b>(a)</b> Number of active connections (i.e. connections on which there actually is something going on).

@subsection section-test_performance_test Performance Test

This isn't a different test, but it describes what we will do on all the different test scenarios.

We will monitor resources used by the proxy process, including cpu and memory usage.
We will also keep a record of:

@li Queries/sec
@li Query times (how long does the same query take to execute)

Each result will include all the information related to each test run, number of connections, threads, etc.
This will show the impact of each setting on used resources.

We will store this information per build. This will allow us to detect regression.


@page variables Variables to tweak (per test scenario).

@li <b>(c)</b> Number of connections.
@li <b>(a)</b> Number of active connections (i.e. connections on which there actually is something going on).
@li <b>(s_l) (size_lower)</b> and <b>(s_u) (size_upper)</b> Size of the resultsets/blobs on each connection, that should be a range.
@li <b>(r)</b> Rate of connection we establish.
@li <b>(t)</b> Number of threads.
@li We will start with no script at all to keep the tests scenarios simple. But we plan on adding Lua scripts later on.


@page information_collected Information to collect.

To start we will output all results to a csv file, for later analysis. We will be able to easily create graphs as well.

We will collect:

@li Query time (with and without proxy in between)
@li Number of queries/seconds (with and without proxy in between)
@li Network traffic  MB/Sec
@li Proxy CPU and Memory usage.
@li All the settings we used to run each test.


@page tools The tools we plan to use are:

@li PHP for scripting and for web applications.
@li jMeter for simulating web users of a web app.
@li More to come as we develop more tests.


*/ 
