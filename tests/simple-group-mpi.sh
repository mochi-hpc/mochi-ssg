#!/bin/bash -x

if [ -z $srcdir ]; then
    echo srcdir variable not set.
    exit 1
fi
source $srcdir/tests/test-util.sh

# launch a group and wait for termination
export SSG_GROUP_LAUNCH_DURATION=10
launch_ssg_group_mpi 4 na+sm &
if [ $? -ne 0 ]; then
    wait
    exit 1
fi

wait $!
if [ $? -ne 0 ]; then
    exit 1
fi

exit 0
