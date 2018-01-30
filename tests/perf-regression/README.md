margo-p2p-latency
---------------------
margo-p2p-latency is a point to point latency benchmark.  It measures round
trip latency for a noop (i.e. as close to an empty request and response
structure as possible) RPC.

Example compile (must build with MPI support):

```
./configure <normal arguments> CC=mpicc
make 
make tests
```

Example execution (requires mpi):

```
mpiexec -n 2 tests/perf-regression/margo-p2p-latency -i 10 -n sm://
```

-i is number of iterations 
-n is transport to use

margo-p2p-bw
---------------------
margo-p2p-bw is a point to point bandwidth benchmark.  It measures Margo
(Mercury) bulk transfer operations in both PULL and PUSH mode and includes
command line arguments to control the concurrency level.

The timing and bandwidth calculation is performed by the client and includes
a round trip RPC to initiate and complete the streaming transfer.

Example compile (must build with MPI support):

```
./configure <normal arguments> CC=mpicc
make 
make tests
```

Example execution (requires mpi):

```
mpiexec -n 2 ./margo-p2p-bw -x 4096 -n sm:// -c 1 -D 10
```

-x is the tranfer size per bulk operation
-n is transport to use
-c is the concurrency level
-D is the duration of the test (for each direction)
