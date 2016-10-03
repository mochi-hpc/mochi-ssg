#!/bin/bash

export LSAN_OPTIONS="exitcode=0"

timeout_cmd="timeout 30s"
# run me from the top-level build dir
pids=()
$timeout_cmd examples/ssg-example-margo -s 1 bmi+tcp://3344 conf ../examples/example.4.conf > example.0.out 2>&1 &
pids[0]=$!
$timeout_cmd examples/ssg-example-margo -s 1 bmi+tcp://3345 conf ../examples/example.4.conf > example.1.out 2>&1 &
pids[1]=$!
$timeout_cmd examples/ssg-example-margo -s 1 bmi+tcp://3346 conf ../examples/example.4.conf > example.2.out 2>&1 &
pids[2]=$!
$timeout_cmd examples/ssg-example-margo -s 1 bmi+tcp://3347 conf ../examples/example.4.conf > example.3.out 2>&1 &
pids[3]=$!

err=0
for pid in ${pids[@]} ; do
    if [[ $err != 0 ]] ; then
        kill $pid
    else
        wait $pid
        err=$?
        if [[ $err != 0 ]] ; then
            echo "ERROR (code $err), killing remaining"
        fi
    fi
done

exit $err
