#!/bin/bash

if [[ -z "$srcdir" ]] ; then
    top_srcdir=../
fi
tdir="$srcdir"/tests

conf0="$tdir"/test.3.0.conf
conf1="$tdir"/test.3.1.conf

timeout_cmd="timeout 30s"
# run me from the top-level build dir
pids=()
$timeout_cmd tests/ssg-test-margo-dblgrp \
    -s 1 0 bmi+tcp://3344 $conf0 $conf1 > test.3.0.out 2>&1 &
pids[0]=$!
$timeout_cmd tests/ssg-test-margo-dblgrp \
    -s 1 0 bmi+tcp://3345 $conf0 $conf1 > test.3.1.out 2>&1 &
pids[1]=$!
$timeout_cmd tests/ssg-test-margo-dblgrp \
    -s 1 0 bmi+tcp://3346 $conf0 $conf1 > test.3.2.out 2>&1 &
pids[2]=$!
$timeout_cmd tests/ssg-test-margo-dblgrp \
    -s 0 1 bmi+tcp://5344 $conf0 $conf1 > test.3.3.out 2>&1 &
pids[3]=$!
$timeout_cmd tests/ssg-test-margo-dblgrp \
    -s 0 1 bmi+tcp://5345 $conf0 $conf1 > test.3.4.out 2>&1 &
pids[4]=$!
$timeout_cmd tests/ssg-test-margo-dblgrp \
    -s 0 1 bmi+tcp://5346 $conf0 $conf1 > test.3.5.out 2>&1 &
pids[5]=$!

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
    rm  test.3.0.out test.3.1.out test.3.2.out \
        test.3.3.out test.3.4.out test.3.5.out
fi
exit $err
