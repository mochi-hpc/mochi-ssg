# SSG (Scalable Service Groups)

SSG is a group membership microservice based on the Mercury RPC system.
It provides mechanisms for bootstrapping sets of Mercury processes into
logical groups and for managing the membership of these process groups
over time. At a high-level, each group collectively maintains a _group view_,
which is just a mapping from group member identifiers to Mercury address
information. The inital group membership view is specified completely
when the group is bootstrapped (created). Currently, SSG offers the
following group bootstrapping methods:

- MPI communicator-based bootstrap
- config file-based bootstrap
- generic bootstrap method using an array of Mercury address strings

Process groups are referenced using unique group identifiers
which encode Mercury address information that can be used to connect
with a representative member of the group. These identifiers may be
transmitted to other processes so they can join the group or attach to
the group (_attachment_ provides non-group members a way to access a
group's view). 

Optionally, SSG can be configured to use the [SWIM failure detection and
group membership protocol](http://www.cs.cornell.edu/~asdas/research/dsn02-SWIM.pdf)
internally to detect and respond to group member failures. SWIM uses a
randomized probing mechanism to detect faulty group members and uses an
efficient gossip protocol to dissmeninate group membership changes to other
group members. SSG propagates group membership view updates back to the SSG
user using a callback interface.

__NOTE__: SSG does not currently support group members dynamically leaving
or joining a group, though this should be supported in the near future.
This means that, for now, SSG groups are immutable after creation.
When using SWIM, this means members can be marked as faulty, but they
cannot be fully evicted from the group view yet.

__NOTE__: SSG does not currently allow for user-specified group member
identifiers, and instead assigns identifiers as dense ranks into the
list of address strings specified at group creation time. That is,
the group member with the first address string in the list is rank 0,
and so on.

## Dependencies

* mercury (git clone --recurse-submodules https://github.com/mercury-hpc/mercury.git)
* argobots (git clone https://github.com/pmodels/argobots.git)
* margo (git clone https://xgitlab.cels.anl.gov/sds/margo.git)
* abt-snoozer (git clone https://xgitlab.cels.anl.gov/sds/abt-snoozer)
* libev (e.g libev-dev package on Ubuntu or Debian)

## Building

(if configuring for the first time)
./prepare.sh

./configure [standard options] PKG\_CONFIG\_PATH=/path/to/pkgconfig/files

make

make install

MPI support is by default optionally included. If you wish to compile with MPI
support, set CC=mpicc (or equivalent) in configure. If you wish to disable MPI
entirely, use --disable-mpi (you can also force MPI inclusion through
--enable-mpi).
