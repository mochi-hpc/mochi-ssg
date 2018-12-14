#
# General test script utilities
#

if [ -z "$MKTEMP" ]; then
    echo "Error: MKTEMP variable should be defined to its respective command"
    exit 1
fi

function parse_launch_args ()
{
    local options=""

    # parse known cmdline options out of env
    if [ ! -z $SSG_GROUP_LAUNCH_NAME ]; then
        options="$options -n $SSG_GROUP_LAUNCH_NAME"
    fi
    if [ ! -z $SSG_GROUP_LAUNCH_DURATION ]; then
        options="$options -s $SSG_GROUP_LAUNCH_DURATION"
    fi
    if [ ! -z $SSG_GROUP_LAUNCH_GIDFILE ]; then
        options="$options -f $SSG_GROUP_LAUNCH_GIDFILE"
    fi

    echo $options
}

function launch_ssg_group_mpi ()
{
    nmembers=${1:-4}
    hg_addr=${2:-"na+sm"}
    options=$(parse_launch_args)

    # launch SSG group given options
    mpirun -np $nmembers tests/ssg-launch-group $options $hg_addr mpi
}

function launch_ssg_group_pmix ()
{
    nmembers=${1:-4}
    hg_addr=${2:-"na+sm"}
    options=$(parse_launch_args)

    prun -n $nmembers tests/ssg-launch-group $options $hg_addr pmix
}
