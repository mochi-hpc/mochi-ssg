#!/bin/bash

# run me from the top-level build dir
mpirun -np 3 examples/ssg-example -s 0 cci+sm://localhost:3344 conf ../examples/example.conf
