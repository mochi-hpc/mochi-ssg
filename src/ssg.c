/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#include <ssg-config.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <mercury.h>
#include <margo.h>

#include <ssg.h>
#include "ssg-internal.h"

#if USE_SWIM_FD
#include "swim-fd/swim-fd.h"
#endif

// internal initialization of ssg data structures
static ssg_t ssg_init_internal(margo_instance_id mid, int self_rank,
    int group_size, hg_addr_t self_addr, char *addr_str_buf);

// lookup peer addresses
static hg_return_t ssg_lookup(ssg_t s, char **addr_strs);
static char** setup_addr_str_list(int num_addrs, char * buf);


ssg_t ssg_init_config(margo_instance_id mid, const char * fname)
{
    // file to read
    int fd = -1;
    struct stat st;

    // file content to parse
    char *rdbuf = NULL;
    ssize_t rdsz;

    // parse metadata (strtok)
    char *tok;

    // vars to build up the addr string list
    int addr_cap = 128;
    int addr_len = 0;
    int num_addrs = 0;
    void *addr_buf = NULL;

    // self rank/addr resolution helpers
    hg_class_t *hgcl = NULL;
    hg_addr_t self_addr = HG_ADDR_NULL;
    char * self_addr_str = NULL;
    const char * self_addr_substr = NULL;
    hg_size_t self_addr_size = 0;
    const char * addr_substr = NULL;
    int rank = -1;

    // misc return codes
    int ret;
    hg_return_t hret;

    // return data
    ssg_t s = NULL;

    // open file for reading
    fd = open(fname, O_RDONLY);
    if (fd == -1) goto fini;

    // get file size
    ret = fstat(fd, &st);
    if (ret == -1) goto fini;

    // slurp file in all at once
    rdbuf = malloc(st.st_size+1);
    if (rdbuf == NULL) goto fini;

    // load it all in one fell swoop
    rdsz = read(fd, rdbuf, st.st_size);
    if (rdsz != st.st_size) goto fini;
    rdbuf[rdsz]='\0';

    hgcl = margo_get_class(mid);
    if(!hgcl) goto fini;

    // get my address
    hret = HG_Addr_self(hgcl, &self_addr);
    if (hret != HG_SUCCESS) goto fini;
    hret = HG_Addr_to_string(hgcl, NULL, &self_addr_size, self_addr);
    if (hret != HG_SUCCESS) goto fini;
    self_addr_str = malloc(self_addr_size);
    if (self_addr_str == NULL) goto fini;
    hret = HG_Addr_to_string(hgcl, self_addr_str, &self_addr_size, self_addr);
    if (hret != HG_SUCCESS) goto fini;

    // strstr is used here b/c there may be inconsistencies in whether the class
    // is included in the address or not (it's not in HG_Addr_to_string, it
    // should be in ssg_init_config)
    self_addr_substr = strstr(self_addr_str, "://");
    if (self_addr_substr == NULL) goto fini;
    self_addr_substr += 3;

    // strtok the result - each space-delimited address is assumed to be
    // a unique mercury address
    tok = strtok(rdbuf, "\r\n\t ");
    if (tok == NULL) goto fini;

    // build up the address buffer
    addr_buf = malloc(addr_cap);
    if (addr_buf == NULL) goto fini;
    do {
        int tok_sz = strlen(tok);
        if (tok_sz + addr_len + 1 > addr_cap) {
            void * tmp;
            addr_cap *= 2;
            tmp = realloc(addr_buf, addr_cap);
            if (tmp == NULL) goto fini;
            addr_buf = tmp;
        }

        // check if this is my addr to resolve rank
        addr_substr = strstr(tok, "://");
        if (addr_substr == NULL) goto fini;
        addr_substr+= 3;
        if (strcmp(self_addr_substr, addr_substr) == 0)
            rank = num_addrs;

        memcpy((char*)addr_buf + addr_len, tok, tok_sz+1);
        addr_len += tok_sz+1;
        num_addrs++;
        tok = strtok(NULL, "\r\n\t ");
    } while (tok != NULL);

    // if rank not resolved, fail
    if (rank == -1) goto fini;

    // init ssg internal structures
    s = ssg_init_internal(mid, rank, num_addrs, self_addr, addr_buf);
    if (s == NULL) goto fini;

    // don't free this on success
    self_addr = HG_ADDR_NULL;
fini:
    if (fd != -1) close(fd);
    free(rdbuf);
    free(addr_buf);
    if (hgcl && self_addr != HG_ADDR_NULL) HG_Addr_free(hgcl, self_addr);
    free(self_addr_str);
    return s;
}

#ifdef HAVE_MPI
ssg_t ssg_init_mpi(margo_instance_id mid, MPI_Comm comm)
{
    // my addr
    hg_class_t *hgcl = NULL;
    hg_addr_t self_addr = HG_ADDR_NULL;
    char * self_addr_str = NULL;
    hg_size_t self_addr_size = 0;
    int self_addr_size_int = 0; // for mpi-friendly conversion

    // collective helpers
    char * addr_buf = NULL;
    int * sizes = NULL;
    int * sizes_psum = NULL;
    int comm_size = 0;
    int comm_rank = 0;

    // misc return codes
    hg_return_t hret;

    // return data
    ssg_t s = NULL;

    hgcl = margo_get_class(mid);
    if(!hgcl) goto fini;

    // get my address
    hret = HG_Addr_self(hgcl, &self_addr);
    if (hret != HG_SUCCESS) goto fini;
    hret = HG_Addr_to_string(hgcl, NULL, &self_addr_size, self_addr);
    if (hret != HG_SUCCESS) goto fini;
    self_addr_str = malloc(self_addr_size);
    if (self_addr_str == NULL) goto fini;
    hret = HG_Addr_to_string(hgcl, self_addr_str, &self_addr_size, self_addr);
    if (hret != HG_SUCCESS) goto fini;
    self_addr_size_int = (int)self_addr_size; // null char included in call

    // gather the buffer sizes
    MPI_Comm_size(comm, &comm_size);
    MPI_Comm_rank(comm, &comm_rank);
    sizes = malloc(comm_size * sizeof(*sizes));
    if (sizes == NULL) goto fini;
    sizes[comm_rank] = self_addr_size_int;
    MPI_Allgather(MPI_IN_PLACE, 0, MPI_BYTE, sizes, 1, MPI_INT, comm);

    // compute a exclusive prefix sum of the data sizes,
    // including the total at the end
    sizes_psum = malloc((comm_size+1) * sizeof(*sizes_psum));
    if (sizes_psum == NULL) goto fini;
    sizes_psum[0] = 0;
    for (int i = 1; i < comm_size+1; i++)
        sizes_psum[i] = sizes_psum[i-1] + sizes[i-1];

    // allgather the addresses
    addr_buf = malloc(sizes_psum[comm_size]);
    if (addr_buf == NULL) goto fini;
    MPI_Allgatherv(self_addr_str, self_addr_size_int, MPI_BYTE,
            addr_buf, sizes, sizes_psum, MPI_BYTE, comm);

    // init ssg internal structures
    s = ssg_init_internal(mid, comm_rank, comm_size, self_addr, addr_buf);
    if (s == NULL) goto fini;

    // don't free these on success
    self_addr = HG_ADDR_NULL;
fini:
    free(sizes);
    free(sizes_psum);
    free(addr_buf);
    if (hgcl && self_addr != HG_ADDR_NULL) HG_Addr_free(hgcl, self_addr);
    free(self_addr_str);
    return s;
}
#endif

static ssg_t ssg_init_internal(margo_instance_id mid, int self_rank,
    int group_size, hg_addr_t self_addr, char *addr_str_buf)
{
    // arrays of peer address strings
    char **addr_strs = NULL;

    // misc return codes
    hg_return_t hret;

    // return data
    ssg_t s = NULL;

    if (self_rank < 0 || self_rank >= group_size || self_addr == HG_ADDR_NULL)
        goto fini;

    // set peer address strings
    addr_strs = setup_addr_str_list(group_size, addr_str_buf);
    if (addr_strs == NULL) goto fini;

    // set up the output
    s = malloc(sizeof(*s));
    if (s == NULL) goto fini;
    memset(s, 0, sizeof(*s));
    s->mid = mid;

    // initialize the group "view"
    s->view.self_rank = self_rank;
    s->view.group_size = group_size;
    s->view.member_states = malloc(group_size * sizeof(*(s->view.member_states)));
    if (s->view.member_states == NULL)
    {
        free(s);
        s = NULL;
        goto fini;
    }
    memset(s->view.member_states, 0, group_size * sizeof(*(s->view.member_states)));
    for (int i = 1; i < group_size; i++)
    {
        int r = (self_rank + i) % group_size;
        // NOTE: remote addrs are set in ssg_lookup
        s->view.member_states[r].addr = HG_ADDR_NULL;
        s->view.member_states[r].is_member = 1;
    }
    // set view info for self
    s->view.member_states[self_rank].addr = self_addr;
    s->view.member_states[self_rank].is_member = 1;

#ifdef DEBUG
    // TODO: log file debug option, instead of just stdout
    s->dbg_strm = stdout;
#endif

    // lookup hg addr information for all group members
    hret = ssg_lookup(s, addr_strs);
    if (hret != HG_SUCCESS)
    {
        ssg_finalize(s);
        s = NULL;
        goto fini;
    }
    SSG_DEBUG(s, "group lookup successful (size=%d)\n", group_size);

#if USE_SWIM_FD
    // initialize swim failure detector
    // TODO: we should probably barrier or sync somehow to avoid rpc failures
    // due to timing skew of different ranks initializing swim
    s->swim_ctx = swim_init(s, 1);
    if (s->swim_ctx == NULL)
    {
        ssg_finalize(s);
        s = NULL;
    }
#endif
    
fini:
    free(addr_strs);
    return s;
}

struct lookup_ult_args
{
    ssg_t ssg;
    int rank;
    char *addr_str;
    hg_return_t out;
};

static void lookup_ult(void *arg)
{
    struct lookup_ult_args *l = arg;
    ssg_t s = l->ssg;

    l->out = margo_addr_lookup(s->mid, l->addr_str,
        &s->view.member_states[l->rank].addr);
    if(l->out != HG_SUCCESS)
        SSG_DEBUG(s, "look up on member %d failed [%d]\n", l->rank, l->out);
}

static hg_return_t ssg_lookup(ssg_t s, char **addr_strs)
{
    ABT_thread *ults;
    struct lookup_ult_args *args;
    int aret;
    hg_return_t hret = HG_SUCCESS;

    if (s == SSG_NULL) return HG_INVALID_PARAM;

    // initialize ULTs
    ults = malloc(s->view.group_size * sizeof(*ults));
    if (ults == NULL) return HG_NOMEM_ERROR;
    args = malloc(s->view.group_size * sizeof(*args));
    if (args == NULL) {
        free(ults);
        return HG_NOMEM_ERROR;
    }
    for (int i = 0; i < s->view.group_size; i++)
        ults[i] = ABT_THREAD_NULL;

    for (int i = 1; i < s->view.group_size; i++) {
        int r = (s->view.self_rank + i) % s->view.group_size;
        args[r].ssg = s;
        args[r].rank = r;
        args[r].addr_str = addr_strs[r];
#if 0
        aret = ABT_thread_create(*margo_get_handler_pool(s->mid), &lookup_ult,
                &args[r], ABT_THREAD_ATTR_NULL, &ults[r]);
        if (aret != ABT_SUCCESS) {
            hret = HG_OTHER_ERROR;
            goto fini;
        }
#endif
    }

    // wait on all
    for (int i = 1; i < s->view.group_size; i++) {
        int r = (s->view.self_rank + i) % s->view.group_size;
#if 1
        aret = ABT_thread_create(*margo_get_handler_pool(s->mid), &lookup_ult,
                &args[r], ABT_THREAD_ATTR_NULL, &ults[r]);
        if (aret != ABT_SUCCESS) {
            hret = HG_OTHER_ERROR;
            goto fini;
        }
#endif
        aret = ABT_thread_join(ults[r]);
        ABT_thread_free(&ults[r]);
        ults[r] = ABT_THREAD_NULL; // in case of cascading failure from join
        if (aret != ABT_SUCCESS) {
            hret = HG_OTHER_ERROR;
            break;
        }
        else if (args[r].out != HG_SUCCESS) {
            hret = args[r].out;
            break;
        }
    }

fini:
    // cleanup
    if (ults != NULL) {
        for (int i = 0; i < s->view.group_size; i++) {
            if (ults[i] != ABT_THREAD_NULL) {
                ABT_thread_cancel(ults[i]);
                ABT_thread_free(ults[i]);
            }
        }
        free(ults);
    }
    if (args != NULL) free(args);

    return hret;
}

void ssg_finalize(ssg_t s)
{
    if (s == SSG_NULL) return;

#if USE_SWIM_FD
    if(s->swim_ctx)
        swim_finalize(s->swim_ctx);
#endif

    for (int i = 0; i < s->view.group_size; i++) {
        if (s->view.member_states[i].addr != HG_ADDR_NULL)
            HG_Addr_free(margo_get_class(s->mid), s->view.member_states[i].addr);
    }
    free(s->view.member_states);
    free(s);
}

int ssg_get_group_rank(const ssg_t s)
{
    return s->view.self_rank;
}

int ssg_get_group_size(const ssg_t s)
{
    return s->view.group_size;
}

hg_addr_t ssg_get_addr(const ssg_t s, int rank)
{
    if (rank >= 0 && rank < s->view.group_size)
        return s->view.member_states[rank].addr;
    else
        return HG_ADDR_NULL;
}

/* -------- */

static char** setup_addr_str_list(int num_addrs, char * buf)
{
    char ** ret = malloc(num_addrs * sizeof(*ret));
    if (ret == NULL) return NULL;

    ret[0] = buf;
    for (int i = 1; i < num_addrs; i++) {
        char * a = ret[i-1];
        ret[i] = a + strlen(a) + 1;
    }
    return ret;
}
