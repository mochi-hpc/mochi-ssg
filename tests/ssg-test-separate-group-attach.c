#include <mpi.h>
#include <ssg.h>
#include <assert.h>

int main(int argc, char **argv)
{
    char ssg_group_buf[256];
    int ret;
    ssg_group_id_t gid;
    margo_instance_id mid;

    MPI_Init(&argc, &argv);

    mid = margo_init(argv[1], MARGO_CLIENT_MODE, 0, 1);
    ssg_init();

    int count=1;
    int actual=0;
    ret = ssg_group_id_load(argv[2], &count, &actual, &gid);
    assert (ret == SSG_SUCCESS);

    fprintf(stderr, "        attaching...\n");
    ret = ssg_group_observe(mid, gid);
    fprintf(stderr, "        attached...\n");

    fprintf(stderr, "        dumping...\n");
    ssg_group_dump(gid);
    fprintf(stderr, "        dumped...\n");

    ssg_group_unobserve(gid);
    margo_finalize(mid);
    MPI_Finalize();
}
