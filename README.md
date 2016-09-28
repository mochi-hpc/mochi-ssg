SSG is a Simple, Static Grouping mechanism for Mercury. It provides
mechanisms for bootstrapping a set of pre-existing mercury processes. So
far, we have the following:

- MPI bootstrap (this works well with CCI, where the addresses you pass in
  aren't the addresses used)
- config-file bootstrap (where each process is assumed to exist in the
  membership list - CCI can't currently be used with this method)

Serializers for the ssg data structure are also provided (untested so far).

# Building

(if configuring for the first time)
./prepare.sh

./configure [standard options] PKG\_CONFIG\_PATH=/path/to/mercury/pkgconfig

make

make install

MPI support is by default optionally included. If you wish to compile with MPI
support, set CC=mpicc (or equivalent) in configure. If you wish to disable MPI
entirely, use --disable-mpi (you can also force MPI inclusion through
--enable-mpi).

# Documentation

The example is the best documentation so far. Check out examples/ssg-example.c
(and the other files) for usage. The example program initializes ssg, pings
peer servers as defined by their ssg rank, and shuts down via a chain
communication.

# Running examples

cd to your build directory and run:

$top\_level\_dir/examples/run-example-\*.sh

See the script contents for how these translate to example execution.
