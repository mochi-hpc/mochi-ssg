#include <stdlib.h>

#include <ssg.h>
#include <ssg-mpi.h>

#define GROUP_NAME "test-group"
#define ASSERT(__cond, __msg, ...) { if(!(__cond)) { fprintf(stderr, "[%s:%d] " __msg, __FILE__, __LINE__, __VA_ARGS__); exit(-1); } }

typedef struct {
    ssg_group_id_t g_id;
    margo_instance_id mid;
} finalize_args_t;

void ssg_finalize_callback(void* arg) {
    fprintf(stderr, "Entering ssg_finalize_callback\n");
    finalize_args_t* a = (finalize_args_t*)arg;
    ssg_group_destroy(a->g_id);
    fprintf(stderr, "Successfully destroyed ssg group\n");
    ssg_finalize();
    fprintf(stderr, "Successfully finalized ssg\n");
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
    ret = ssg_group_create_mpi(mid, GROUP_NAME, MPI_COMM_WORLD, NULL, NULL, NULL, &gid);
    if (ret != SSG_SUCCESS) {
        fprintf(stderr, "ssg_group_create_mpi failed\n");
        exit(-1);
    }

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0)
	ssg_group_id_store(argv[2], gid, 1);

    finalize_args_t args = {
        .g_id = gid,
        .mid = mid
    };
    margo_push_prefinalize_callback(mid, ssg_finalize_callback, (void*)&args);

    fprintf(stderr, "   just hanging out waiting for shutdown\n");

    margo_wait_for_finalize(mid);
    fprintf(stderr, "   .. done waiting: now cleaning up\n");
    ssg_group_destroy(gid);
    fprintf(stderr, "bye\n");

    MPI_Finalize();
}
