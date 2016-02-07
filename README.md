hlld [![Build Status](https://travis-ci.org/armon/hlld.png?branch=master)](https://travis-ci.org/armon/hlld)
=========

hlld is a high-performance C server which is used
to expose HyperLogLog sets and operations over them to
networked clients. It uses a simple ASCI protocol
which is human readable, and similar to memcached.

HyperLogLog's are a relatively new sketching data structure.
They are used to estimate cardinality, i.e. the unique number
of items in a set. They are based on the observation that any
bit in a "good" hash function is independent of any other
bit and that the probability of getting a string of N
bits all set to the same value is 1/(2^N). There is a lot more in
the math, but that is the basic intuition. What is even more
incredible is that the storage required to do the counting
is log(log(N)). So with a 6 bit register, we can count well into
the trillions. For more information, its best to read the papers
referenced at the end.

TL;DR: HyperLogLogs enable you to have a set with about 1.6% variance,
using 3280 bytes, and estimate sizes in the trillions.


Features
--------

* Scalable non-blocking core allows for many connected
  clients and concurrent operations
* Implements 6bit wide HyperLogLogs, allowing almost unbounded counts
* Supports asynchronous flushes to disk for persistence
* Supports non-disk backed sets for high I/O
* Automatically faults cold sets out of memory to save resources
* Dead simple to start and administer
* FAST, FAST, FAST

Install
-------

Download and build from source:

    $ git clone https://armon@github.com/armon/hlld.git
    $ cd hlld
    $ pip install SCons  # Uses the Scons build system, may not be necessary
    $ scons
    $ ./hlld

This will generate some errors related to building the test code
as it depends on libcheck. To build the test code successfully,
do the following:

    $ cd deps/check-0.9.8/
    $ ./configure
    $ make
    # make install
    # ldconfig (necessary on some Linux distros)

Then re-build hlld. At this point, the test code should build
successfully.

Usage
-----

hlld can be configured using a file which is in INI format.
Here is an example configuration file:

    # Settings for hlld
    [hlld]
    tcp_port = 4553
    data_dir = /mnt/hlld
    log_level = INFO
    flush_interval = 60
    default_eps = 0.02
    workers = 2


Then run hlld, pointing it to that file:

    hlld -f /etc/hlld.conf

A full list of configuration options is below.

Clients
----------

Here is a list of known client implementations:

* Go: https://github.com/armon/go-hlld
* Perl: https://github.com/Weborama/Net-Hlld
* Python : https://github.com/armon/pyhlld
* Ruby: https://github.com/mdlayher/rb-hlld
* Node.js: https://github.com/JamesS237/node-hlld

Here is a list of "best-practices" for client implementations:

* Maintain a set of open connections to the server to minimize connection time
* Make use of the bulk operations when possible, as they are more efficient.
* For long keys, it is better to do a client-side hash (SHA1 at least), and send
  the hash as the key to minimize network traffic.


Configuration Options
---------------------

Each configuration option is documented below:

 * tcp\_port : Integer, sets the tcp port to listen on. Default 4553.

 * port: Same as above. For compatibility.

 * udp\_port : Integer, sets the udp port. Currently listened on
                but otherwise unused. Default 4554.

 * bind\_address: The IP address to bind on. Defaults to 0.0.0.0.

 * data\_dir : The data directory that is used. Defaults to /tmp/hlld

 * log\_level : The logging level that hlld should use. One of:
    DEBUG, INFO, WARN, ERROR, or CRITICAL. All logs go to syslog,
    and stderr if that is a TTY. Default is INFO.

 * workers : This controls the number of worker threads that are used.
   Defaults to 1. If many different sets are used, it can be advantageous
   to increase this to the number of CPU cores. If only a few sets are used,
   the increased lock contention may reduce throughput, and a single worker
   may be better.

 * flush\_interval : This is the time interval in seconds in which
    sets are flushed to disk. Defaults to 60 seconds. Set to 0 to
    disable.

 * cold\_interval : If a set is not accessed (set or bulk), for
    this amount of time, it is eligible to be removed from memory
    and left only on disk. If a set is accessed, it will automatically
    be faulted back into memory. Set to 3600 seconds by default (1 hour).
    Set to 0 to disable cold faulting.

 * in\_memory : If set to 1, then all sets are in-memory ONLY by
    default. This means they are not persisted to disk, and are not
    eligible for cold fault out. Defaults to 0.

 * use\_mmap : If set to 1, the hlld internal buffer management
    is disabled, and instead buffers use a plain mmap() and rely on
    the kernel for all management. This increases data safety in the
    case that hlld crashes, but has adverse affects on performance
    if the total memory utilization of the system is high. In general,
    this should be left to 0, which is the default.

 * default\_eps: If not provided to create, this is the default
    error of the HyperLogLog. This is an upper bound and is used to
    compute the precision that should be used. This option overrides
    a given default precision. Defaults to 1.625%, which is a precision
    of 12. Only one of default\_eps or default\_precision should be provided.

 * default\_precision : If not provided to create, this is the default
    "precision" of the HyperLogLog. This controls the error in the size
    estimate. This option overrides a given default eps. Defaults to 12,
    which is results in a variance of about 1.625%. Only one of default\_eps
    or default\_precision should be provided.


It is important to note that reducing the error bound increases the
required precision. The size utilization of a HyperLogLog increases
exponentially with the precision, so it should be increased carefully.

Protocol
--------

By default, hlld will listen for TCP connections on port 4553.
It uses a simple ASCII protocol that is very similar to memcached.

A command has the following syntax::

    cmd [args][\r]\n

We start each line by specifying a command, providing optional arguments,
and ending the line in a newline (carriage return is optional).

There are a total of 9 commands:

* create - Create a new set (a set is a named HyperLogLog)
* list - List all sets or those matching a prefix
* drop - Drop a set (Deletes from disk)
* close - Closes a set (Unmaps from memory, but still accessible)
* clear - Clears a set from the lists (Removes memory, left on disk)
* set|s - Set an item in a set
* bulk|b - Set many items in a set at once
* info - Gets info about a set
* flush - Flushes all sets or just a specified one

For the ``create`` command, the format is::

    create set_name [precision=prec] [eps=max_eps] [in_memory=0|1]

Where ``set_name`` is the name of the set,
and can contain the characters a-z, A-Z, 0-9, ., _.
If a precision is provided the set
will be created with the given bits of precision, otherwise the configured default value will be used.
If a maximum epsilon is provided, that will be used to compute a precision, otherwise the configured default is used.
You can optionally specify in_memory to force the set to not be persisted to disk. If both precision and
eps are specified, it is not specified which one will be used. Generally, only one should be provided,
as the other will be computed.

As an example::

    create foobar eps=0.01

This will create a set foobar that has a maximum variance of 1%.
Valid responses are either "Done", "Exists", or "Delete in progress". The last response
occurs if a set of the same name was recently deleted, and hlld
has not yet completed the delete operation. If so, a client should
retry the create in a few seconds.

The ``list`` command takes either no arguments or a set prefix, and returns information
about the matching sets.

For example, doing:

    list foo

Will return a list of all sets with the foo prefix. Here is an example response:

    START
    foobar 0.010000 14 13108 0
    END

This indicates a single set named foobar, with a variance
of 0.01, precision 14, a 13108 byte size, a current size estimate of 0
items.

The ``drop``, ``close`` and ``clear`` commands are like create, but only takes a set name.
It can either return "Done" or "Set does not exist". ``clear`` can also return "Set is not proxied. Close it first.".
This means that the set is still in-memory and not qualified for being cleared.
This can be resolved by first closing the set.

set is a very simple command:

    set set_name key

The command must specify a set and a key to use.
It will either return "Done", or "Set does not exist".

The bulk command is similar to set but allows for many keys
to be set at once. Keys must be separated by a space:

    bulk set_name key1 [key_2 [key_3 [key_N]]]

The bulk and set commands can also be called by their aliases
b and s respectively.

The ``info`` command takes a set name, and returns
information about the set. Here is an example output:

    START
    in_memory 1
    page_ins 0
    page_outs 0
    eps 0.02
    precision 12
    sets 0
    size 1540
    storage 3280
    END

The command may also return "Set does not exist" if the set does
not exist.

The ``flush`` command may be called without any arguments, which
causes all sets to be flushed. If a set name is provided
then that set will be flushed. This will either return "Done" or
"Set does not exist".

Example
----------

Here is an example of a client flow, assuming hlld is
running on the default port using just telnet::

    $ telnet localhost 4553
    > list
    START
    END

    > create foobar
    Done

    > set foobar zipzab
    Done

    > bulk foobar zipzab blah boo
    Done

    > list
    START
    foobar 0.016250 12 3280 3
    END

    > drop foobar
    Done

    > list
    START
    END


Performance
-----------

Although extensive performance evaluations have not been done,
casual testing on a 2012 MBP with pure set operations
allows for a throughput of at least 1MM ops/sec. On Linux,
response times can be as low as 1 μs.

hlld also supports multi-core systems for scalability, so
it is important to tune it for the given work load. The number
of worker threads can be configured either in the configuration
file, or by providing a `-w` flag. This should be set to at most
2 * CPU count. By default, only a single worker is used.


References
-----------

Here are some related works which we make use of:

* HyperLogLog in Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm : http://research.google.com/pubs/pub40671.html
* HyperLogLog: The analysis of a near-optimal cardinality estimation algorithm : http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.142.9475

