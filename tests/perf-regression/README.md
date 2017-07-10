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
