#include <ssg.h>
#include <ssg-mpi.h>

#define GROUP_NAME "test-group"
#define ASSERT(__cond, __msg, ...) { if(!(__cond)) { fprintf(stderr, "[%s:%d] " __msg, __FILE__, __LINE__, __VA_ARGS__); exit(-1); } }

static void finalized_ssg_group_cb(void* data)
{
    ssg_group_id_t gid = *((ssg_group_id_t*)data);
    ssg_group_destroy(gid);
}


int main(int argc, char **argv)
{
    int ret, rank;
    ssg_group_id_t gid;
    margo_instance_id mid;
    mid = margo_init(argv[1], MARGO_SERVER_MODE, 0, 1);
    margo_enable_remote_shutdown(mid);

    hg_addr_t my_address;
    margo_addr_self(mid, &my_address);
    char addr_str[128];
    size_t addr_str_size = 128;
    margo_addr_to_string(mid, addr_str, &addr_str_size, my_address);
    margo_addr_free(mid,my_address);
    printf("Server running at address %s\n", addr_str);

    MPI_Init(&argc, &argv);
    ret = ssg_init();
    ASSERT(ret == 0, "ssg_init failed (ret = %d)\n", ret);
    gid = ssg_group_create_mpi(mid, GROUP_NAME, MPI_COMM_WORLD, NULL, NULL, NULL);
    if (gid == SSG_GROUP_ID_INVALID) {
        fprintf(stderr, "ssg_group_create_mpi failed\n");
        exit(-1);
    }
    margo_push_finalize_callback(mid, &finalized_ssg_group_cb, (void*)&gid);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0)
	ssg_group_id_store(argv[2], gid, 1);

    fprintf(stderr, "   just hanging out...\n");
    margo_wait_for_finalize(mid);
    ssg_group_destroy(gid);
    margo_finalize(mid);

    MPI_Finalize();
}
