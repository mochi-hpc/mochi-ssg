#!/bin/bash

# run me from the top-level build dir
mpirun -np 3 tests/ssg-test -s 0 cci+sm mpi
