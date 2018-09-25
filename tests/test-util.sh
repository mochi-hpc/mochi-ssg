#
# General test script utilities
#

if [ -z "$MKTEMP" ]; then
    echo "Error: MKTEMP variable should be defined to its respective command"
    exit 1
fi

function launch_ssg_group_mpi ()
{
    nmembers=${1:-4}
    hg_addr=${2:-"na+sm"}
    options=""

    # parse known cmdline options out of env
    if [ ! -z $SSG_GROUP_LAUNCH_NAME ]; then
        options="$options -n $SSG_GROUP_LAUNCH_NAME"
    fi
    if [ ! -z $SSG_GROUP_LAUNCH_DURATION ]; then
        options="$options -d $SSG_GROUP_LAUNCH_DURATION"
    fi
    if [ ! -z $SSG_GROUP_LAUNCH_GIDFILE ]; then
        options="$options -f $SSG_GROUP_LAUNCH_GIDFILE"
    fi

    # launch SSG group given options
    mpirun -np $nmembers tests/ssg-launch-group $options $hg_addr mpi
}

function join_ssg_group ()
{
    hg_addr=${1:-"na+sm"}
    gid_file=${2}
    options=""

    if [ -z "$gid_file" ]; then
        echo "Error: join_ssg_group requires a valid GID file argument"
        exit 1
    fi

    # parse known cmdline options out of env
    if [ ! -z $SSG_GROUP_LAUNCH_DURATION ]; then
        options="$options -d $SSG_GROUP_LAUNCH_DURATION"
    fi

    tests/ssg-join-group $options $hg_addr $gid_file
}
