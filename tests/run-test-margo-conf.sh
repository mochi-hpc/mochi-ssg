#!/bin/bash

if [[ -z "$srcdir" ]] ; then
    srcdir=..
fi
tdir="$srcdir"/tests

timeout_cmd="timeout 30s"
# run me from the top-level build dir
pids=()
$timeout_cmd tests/ssg-test-margo -s 1 bmi+tcp://3344 conf "$tdir"/test.4.conf > test.0.out 2>&1 &
pids[0]=$!
$timeout_cmd tests/ssg-test-margo -s 1 bmi+tcp://3345 conf "$tdir"/test.4.conf > test.1.out 2>&1 &
pids[1]=$!
$timeout_cmd tests/ssg-test-margo -s 1 bmi+tcp://3346 conf "$tdir"/test.4.conf > test.2.out 2>&1 &
pids[2]=$!
$timeout_cmd tests/ssg-test-margo -s 1 bmi+tcp://3347 conf "$tdir"/test.4.conf > test.3.out 2>&1 &
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

if [[ $err == 0 ]] ; then
    rm test.0.out test.1.out test.2.out test.3.out
fi

exit $err
