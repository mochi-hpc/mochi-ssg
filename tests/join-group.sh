#!/bin/bash -x

if [ -z $srcdir ]; then
    echo srcdir variable not set.
    exit 1
fi
source $srcdir/tests/test-util.sh

TMPOUT=$($MKTEMP -d --tmpdir test-XXXXXX)

# launch initial group, storing GID
export SSG_GROUP_LAUNCH_NAME=simplest-group
export SSG_GROUP_LAUNCH_DURATION=10
export SSG_GROUP_LAUNCH_GIDFILE=$TMPOUT/gid.out
launch_ssg_group_mpi 4 na+sm
if [ $? -ne 0 ]; then
    wait
    rm -rf $TMPOUT
    exit 1
fi

sleep 2

# try to join running group
export SSG_GROUP_LAUNCH_DURATION=8
join_ssg_group na+sm $SSG_GROUP_LAUNCH_GIDFILE
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
