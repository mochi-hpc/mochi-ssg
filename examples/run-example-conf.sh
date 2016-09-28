#!/bin/bash

pids=()
# run me from the top-level build dir
examples/ssg-example -s 2 bmi+tcp://3344 conf ../examples/example.conf > example.0.out 2>&1 &
pids[0]=$!
examples/ssg-example -s 2 bmi+tcp://3345 conf ../examples/example.conf > example.1.out 2>&1 &
pids[1]=$!
examples/ssg-example -s 2 bmi+tcp://3346 conf ../examples/example.conf > example.2.out 2>&1 &
pids[2]=$!

err=0
for pid in ${pids[@]} ; do
    if [[ $err != 0 ]] ; then
        kill $pid
    else
        wait $pid
        err=$?
        if [[ $err != 0 ]] then
            echo "ERROR (code $err), killing remaining"
        fi
    fi
done

if [[ $err == 0 ]] ; then rm example.0.out example.1.out example.2.out ; fi

exit $err
