#!/bin/bash

# run me from the top-level build dir
examples/ssg-example -s 2 bmi+tcp://localhost:3344 conf ../examples/example.conf &
examples/ssg-example -s 2 bmi+tcp://localhost:3345 conf ../examples/example.conf &
examples/ssg-example -s 2 bmi+tcp://localhost:3346 conf ../examples/example.conf
