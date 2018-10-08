#!/bin/bash -x

if [ -z $srcdir ]; then
    echo srcdir variable not set.
    exit 1
fi
source $srcdir/tests/test-util.sh

TMPOUT=$($MKTEMP -d --tmpdir test-XXXXXX)

export SSG_DEBUG_LOGDIR=$TMPOUT

# launch initial group, storing GID
export SSG_GROUP_LAUNCH_DURATION=30
export SSG_GROUP_LAUNCH_GIDFILE=gid.out
launch_ssg_group_mpi 4 na+sm &
if [ $? -ne 0 ]; then
    wait
    rm -rf $TMPOUT
    exit 1
fi

sleep 5

tests/ssg-join-leave-group -s 25 -l 10 na+sm $SSG_GROUP_LAUNCH_GIDFILE &
if [ $? -ne 0 ]; then
    wait
    rm -rf $TMPOUT
    exit 1
fi

wait
if [ $? -ne 0 ]; then
    rm -rf $TMPOUT
    exit 1
fi

rm -rf $TMPOUT
exit 0
