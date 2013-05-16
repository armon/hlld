hlld [![Build Status](https://travis-ci.org/armon/hlld.png?branch=master)](https://travis-ci.org/armon/hlld)
=========

hlld is a high-performance C server which is used
to expose HyperLogLog sets and operations over them to
networked clients. It uses a simple ASCI protocol
which is human readable, and similar to memcached.

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

Download and build from source::

    $ git clone https://armon@github.com/armon/hlld.git
    $ cd hlld
    $ pip install SCons  # Uses the Scons build system, may not be necessary
    $ scons
    $ ./hlld

This will generate some errors related to building the test code
as it depends on libcheck. To build the test code successfully,
do the following::

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

::

    # Settings for hlld
    [hlld]
    tcp_port = 4553
    udp_port = 4554
    data_dir = /mnt/hlld
    log_level = INFO
    cold_interval = 3600
    flush_interval = 60
    default_eps = 0.02
    workers = 2


Then run hlld, pointing it to that file::

    hlld -f /etc/hlld.conf

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
* list - List all sets
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

The ``list`` command takes no arguments, and returns information
about all the sets. Here is an example response::

    START
    foobar 0.02 12 4096 1540
    END

This indicates a single set named foobar, with a variance
of 0.02, precision 12, a 4096 byte size, a current size estimate of 1540
items.

The ``drop``, ``close`` and ``clear`` commands are like create, but only takes a set name.
It can either return "Done" or "Set does not exist". ``clear`` can also return "Set is not proxied. Close it first.".
This means that the set is still in-memory and not qualified for being cleared.
This can be resolved by first closing the set.

set is a very simple command::

    set set_name key

The command must specify a set and a key to use.
It will either return "Done", or "Set does not exist".

The bulk command is similar to set but allows for many keys
to be set at once. Keys must be separated by a space::

    bulk set_name key1 [key_2 [key_3 [key_N]]]

The bulk and set commands can also be called by their aliasses
b and s respectively.

The ``info`` command takes a set name, and returns
information about the set. Here is an example output::

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
    foobar 0.02 12 4096 3
    END

    > drop foobar
    Done

    > list
    START
    END


Clients
----------

Here is a list of known client implementations:

* WIP...

Here is a list of "best-practices" for client implementations:

* Maintain a set of open connections to the server to minimize connection time
* Make use of the bulk operations when possible, as they are more efficient.
* For long keys, it is better to do a client-side hash (SHA1 at least), and send
  the hash as the key to minimize network traffic.


References
-----------

Here are some related works which we make use of:

* HyperLogLog in Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm : http://research.google.com/pubs/pub40671.html
* HyperLogLog: The analysis of a near-optimal cardinality estimation algorithm : http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.142.9475

