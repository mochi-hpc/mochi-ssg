/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#include <ssg-config.h>

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#ifdef SSG_HAVE_MPI
#include <mpi.h>
#endif

#include <margo.h>
#include <ssg.h>

#define DIE_IF(cond_expr, err_fmt, ...) \
    do { \
        if (cond_expr) { \
            fprintf(stderr, "ERROR at %s:%d (" #cond_expr "): " \
                    err_fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
            exit(1); \
        } \
    } while(0)

#include <time.h>
double my_wtime() {
    struct timeval time;
    gettimeofday(&time, NULL);
    return time.tv_sec + time.tv_usec/1000000.0;
}

DECLARE_MARGO_RPC_HANDLER(group_id_forward_recv_ult)

static void usage()
{
    fprintf(stderr,
        "Usage: "
        "ssg-refresh-group <addr> <GID>\n"
        "Observe group given by GID using Mercury address ADDR.\n");
}

static void parse_args(int argc, char *argv[], const char **addr_str, const char **gid_file)
{
    int ndx = 1;

    if (argc < 2)
    {
        usage();
        exit(1);
    }

    *addr_str = argv[ndx++];
    *gid_file = argv[ndx++];

    return;   
}

void dump_histogram(double *values, int count)
{
#define NR_BINS 5
    int i;
    double max=values[0];
    double min=values[0];
    int bins[NR_BINS];

    if (count == 1)
        return; // histograms not needed for single value

    for (i=0; i< NR_BINS; i++)
        bins[i] = 0;

    for (i=1; i<count; i++) {
        if (values[i] > max)
            max = values[i];
        if (values[i] < min) {
            min = values[i];
        }
    }

    double step = (max-min)/(double)NR_BINS;

    for (i=0; i< count; i++) {
        int bin = (values[i]-min)/step;
        if (bin == NR_BINS) bin--;
        bins[bin]++;
    }
    for(i=0; i< NR_BINS; i++)
        printf("%f-%f : %d\n", min+step*i, min+step*i+step, bins[i]);
}
int main(int argc, char *argv[])
{
    margo_instance_id mid = MARGO_INSTANCE_NULL;
    const char *addr_str;
    const char *gid_file;
    ssg_group_id_t g_id;
    int num_addrs;
    double load_time, refresh_time;
    int nprocs=0, rank=0;
    ssg_group_id_t member_id;
    int member_rank;
    int group_size;
    hg_addr_t member_addr;
    int sret;

    parse_args(argc, argv, &addr_str, &gid_file);

#ifdef SSG_HAVE_MPI
    double min_load, max_load, min_refresh, max_refresh, sum_load, sum_refresh;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
#endif

    /* init margo */
    /* use the main xstream to drive progress & run handlers */
    mid = margo_init(addr_str, MARGO_CLIENT_MODE, 0, -1);
    DIE_IF(mid == MARGO_INSTANCE_NULL, "margo_init");

    /* initialize SSG */
    sret = ssg_init();
    DIE_IF(sret != SSG_SUCCESS, "ssg_init (%s)", ssg_strerror(sret));

    /* load group info from file */
    num_addrs = SSG_ALL_MEMBERS;
    load_time = my_wtime();
    sret = ssg_group_id_load(gid_file, &num_addrs, &g_id);
    load_time = my_wtime() - load_time;
    DIE_IF(sret != SSG_SUCCESS, "ssg_group_id_load (%s)", ssg_strerror(sret));
    DIE_IF(num_addrs < 1, "ssg_group_id_load (%s)", ssg_strerror(sret));

    /* assert some things about loaded group */
    sret = ssg_get_group_size(g_id, &group_size);
    DIE_IF(sret != SSG_SUCCESS, "ssg_get_group_size (%s)", ssg_strerror(sret));
    sret = ssg_get_group_member_id_from_rank(g_id, 0, &member_id);
    DIE_IF(sret != SSG_SUCCESS, "ssg_get_group_member_id_from_rank (%s)", ssg_strerror(sret));
    sret = ssg_get_group_member_rank(g_id, member_id, &member_rank);
    DIE_IF(sret != SSG_SUCCESS, "ssg_get_group_member_rank (%s)", ssg_strerror(sret));
    DIE_IF(member_rank != 0, "ssg_get_group_member_rank (%s)", ssg_strerror(sret));
    /* get_group_member_addr() will fail since we do not have a margo
     * instance associated with the group, which happens later as
     * part of group_refresh()
     */
    sret = ssg_get_group_member_addr(g_id, member_id, &member_addr);
    DIE_IF(sret != SSG_ERR_MID_NOT_FOUND, "ssg_get_group_member_addr (%s)", ssg_strerror(sret));
    DIE_IF(member_addr != HG_ADDR_NULL, "ssg_get_group_member_addr (%s)", ssg_strerror(sret));

    refresh_time = my_wtime();
    /* refresh the SSG server group view */
    sret = ssg_group_refresh(mid, g_id);
    refresh_time = my_wtime() - refresh_time;
    DIE_IF(sret != SSG_SUCCESS, "ssg_group_refresh (%s)", ssg_strerror(sret));

    /* With a large number of clients, having everyone dump their group state
     * gets unwieldy. */
    if (rank == 0) ssg_group_dump(g_id);

    /* clean up */
    ssg_group_destroy(g_id);
    ssg_finalize();
    margo_finalize(mid);

#ifdef SSG_HAVE_MPI
    MPI_Reduce(&load_time, &max_load, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&load_time, &min_load, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&load_time, &sum_load, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    MPI_Reduce(&refresh_time, &max_refresh, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&refresh_time, &min_refresh, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&refresh_time, &sum_refresh, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    /* histogram of results */
    double *all_refreshes;
    all_refreshes=malloc(nprocs*sizeof(*all_refreshes));
    MPI_Gather(&refresh_time, 1, MPI_DOUBLE,
            all_refreshes, 1, MPI_DOUBLE,
            0, MPI_COMM_WORLD);

   if (rank == 0) {

        dump_histogram(all_refreshes, nprocs);
        for (int i=0; i< nprocs; i++) printf("%f ", all_refreshes[i]);
        printf("\n");

        printf(" %d : load average (min max): %f ( %f %f )\n", nprocs, sum_load/nprocs, min_load, max_load);
        printf(" %d : refresh average (min max): %f ( %f %f )\n", nprocs, sum_refresh/nprocs, min_refresh, max_refresh);
    }

    MPI_Finalize();
#endif

    return 0;
}
