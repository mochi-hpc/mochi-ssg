#include <mpi.h>
#include <ssg.h>
#include <assert.h>

int main(int argc, char **argv)
{
    int ret;
    ssg_member_id_t member_id;
    hg_addr_t remote_addr = HG_ADDR_NULL;
    ssg_group_id_t gid;
    margo_instance_id mid;

    MPI_Init(&argc, &argv);

    mid = margo_init(argv[1], MARGO_CLIENT_MODE, 0, 1);
    ssg_init();

    int count=1;
    ret = ssg_group_id_load(argv[2], &count, &gid);
    assert (ret == SSG_SUCCESS);

    fprintf(stderr, "        refreshing...\n");
    ret = ssg_group_refresh(mid, gid);
    fprintf(stderr, "        refreshed...\n");

    fprintf(stderr, "        dumping...\n");
    ssg_group_dump(gid);
    fprintf(stderr, "        dumped...\n");

    ret = ssg_get_group_member_id_from_rank(gid, 0, &member_id);
    assert(ret == SSG_SUCCESS);
    ret = ssg_get_group_member_addr(gid, member_id, &remote_addr);
    assert(ret == SSG_SUCCESS);

    ret = margo_shutdown_remote_instance(mid, remote_addr);
    assert (ret == HG_SUCCESS);

    ssg_group_destroy(gid);
    ssg_finalize();

    margo_finalize(mid);
    MPI_Finalize();
}
