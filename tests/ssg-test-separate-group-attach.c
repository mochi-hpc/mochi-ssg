#include <mpi.h>
#include <ssg.h>
#include <assert.h>

int main(int argc, char **argv)
{
    int ret;
    hg_addr_t remote_addr = HG_ADDR_NULL;
    ssg_group_id_t gid;
    margo_instance_id mid;

    MPI_Init(&argc, &argv);

    mid = margo_init(argv[1], MARGO_CLIENT_MODE, 0, 1);
    ssg_init();

    int count=1;
    ret = ssg_group_id_load(argv[2], &count, &gid);
    assert (ret == SSG_SUCCESS);

    fprintf(stderr, "        attaching...\n");
    ret = ssg_group_observe(mid, gid);
    fprintf(stderr, "        attached...\n");

    fprintf(stderr, "        dumping...\n");
    ssg_group_dump(gid);
    fprintf(stderr, "        dumped...\n");

    remote_addr = ssg_get_group_member_addr(gid, ssg_get_group_member_id_from_rank(gid, 0));
    assert(remote_addr != HG_ADDR_NULL);

    ret = margo_shutdown_remote_instance(mid, remote_addr);
    assert (ret == HG_SUCCESS);

    ssg_group_unobserve(gid);
    ssg_finalize();

    margo_finalize(mid);
    MPI_Finalize();
}
