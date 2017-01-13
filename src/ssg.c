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
    int group_size, hg_addr_t self_addr, char *addr_str_buf, int addr_str_buf_size);

// lookup peer addresses
static hg_return_t ssg_lookup(ssg_t s);
static char** setup_addr_str_list(int num_addrs, char * buf);

#if 0
// helper for hashing (don't want to pull in jenkins hash)
// see http://www.isthe.com/chongo/tech/comp/fnv/index.html
static uint64_t fnv1a_64(void *data, size_t size);

MERCURY_GEN_PROC(barrier_in_t,
        ((int32_t)(barrier_id)) \
        ((int32_t)(rank)))

// barrier RPC decls
static void proc_barrier(void *arg);
DEFINE_MARGO_RPC_HANDLER(proc_barrier)
#endif


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
    s = ssg_init_internal(mid, rank, num_addrs, self_addr, addr_buf, addr_len);
    if (s == NULL) goto fini;

    // don't free this on success
    addr_buf = NULL;
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
    s = ssg_init_internal(mid, comm_rank, comm_size, self_addr,
        addr_buf, sizes_psum[comm_size]);
    if (s == NULL) goto fini;

    // don't free these on success
    addr_buf = NULL;
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
    int group_size, hg_addr_t self_addr, char *addr_str_buf, int addr_str_buf_size)
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
    s->addr_str_buf = addr_str_buf;
    s->addr_str_buf_size = addr_str_buf_size;

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
        s->view.member_states[r].status = SSG_MEMBER_UNKNOWN;
        // NOTE: remote addrs are set in ssg_lookup
        s->view.member_states[r].addr = HG_ADDR_NULL;
        s->view.member_states[r].addr_str = addr_strs[r];
    }
    // set view info for self
    s->view.member_states[self_rank].status = SSG_MEMBER_ALIVE;
    s->view.member_states[self_rank].addr = self_addr;
    s->view.member_states[self_rank].addr_str = addr_strs[self_rank];

#ifdef DEBUG
    // TODO: log file debug option, instead of just stderr
    s->dbg_strm = stderr;
#endif

#if 0
    s->barrier_mutex = ABT_MUTEX_NULL;
    s->barrier_cond  = ABT_COND_NULL;
    s->barrier_eventual = ABT_EVENTUAL_NULL;
#endif

    // lookup hg addr information for all group members
    hret = ssg_lookup(s);
    if (hret != HG_SUCCESS)
    {
        /* TODO: is finalize needed? or just free? */
        ssg_finalize(s);
        s = NULL;
        goto fini;
    }

#if USE_SWIM_FD
    // initialize swim failure detector
    s->swim_ctx = swim_init(s->mid, s, 1);
    if (s->swim_ctx == NULL)
    {
        ssg_finalize(s); s = NULL;
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
    hg_return_t out;
};

static void lookup_ult(void *arg)
{
    struct lookup_ult_args *l = arg;
    ssg_t s = l->ssg;

    SSG_DEBUG(s, "looking up rank %d\n", l->rank);
    l->out = margo_addr_lookup(s->mid, s->view.member_states[l->rank].addr_str,
            &s->view.member_states[l->rank].addr);
    SSG_DEBUG(s, "looked up rank %d\n", l->rank);
}

static hg_return_t ssg_lookup(ssg_t s)
{
    hg_context_t *hgctx;
    ABT_thread *ults;
    struct lookup_ult_args *args;
    hg_return_t hret = HG_SUCCESS;

    if (s == SSG_NULL) return HG_INVALID_PARAM;

    // set the hg class up front - need for destructing addrs
    hgctx = margo_get_context(s->mid);
    if (hgctx == NULL) return HG_INVALID_PARAM;

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

        int aret = ABT_thread_create(*margo_get_handler_pool(s->mid), &lookup_ult,
                &args[r], ABT_THREAD_ATTR_NULL, &ults[r]);
        if (aret != ABT_SUCCESS) {
            hret = HG_OTHER_ERROR;
            goto fini;
        }
    }

    // wait on all
    for (int i = 1; i < s->view.group_size; i++) {
        int r = (s->view.self_rank + i) % s->view.group_size;
        int aret = ABT_thread_join(ults[r]);
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

#if 0
// TODO: handle hash collision, misc errors
void ssg_register_barrier(ssg_t s, hg_class_t *hgcl)
{
    if (s->num_addrs == 1) return;

    s->barrier_rpc_id = fnv1a_64(s->backing_buf, s->buf_size);
    hg_return_t hret = HG_Register(hgcl, s->barrier_rpc_id,
            hg_proc_barrier_in_t, NULL, &proc_barrier_handler);
    assert(hret == HG_SUCCESS);
    hret = HG_Register_data(hgcl, s->barrier_rpc_id, s, NULL);
    assert(hret == HG_SUCCESS);

    int aret = ABT_mutex_create(&s->barrier_mutex);
    assert(aret == ABT_SUCCESS);
    aret = ABT_cond_create(&s->barrier_cond);
    assert(aret == ABT_SUCCESS);
    aret = ABT_eventual_create(0, &s->barrier_eventual);
    assert(aret == ABT_SUCCESS);
}

// TODO: process errors in a sane manner
static void proc_barrier(void *arg)
{
    barrier_in_t in;
    hg_return_t hret;
    int aret;
    hg_handle_t h = arg;
    struct hg_info *info = HG_Get_info(h);
    ssg_t s = HG_Registered_data(info->hg_class, info->id);

    assert(s->rank == 0);

    hret = HG_Get_input(h, &in);
    assert(hret == HG_SUCCESS);

    DEBUG("%d: barrier ult: rx round %d from %d\n", s->rank, in.barrier_id,
            in.rank);
    // first wait until the nth barrier has been processed
    aret = ABT_mutex_lock(s->barrier_mutex);
    assert(aret == ABT_SUCCESS);
    while (s->barrier_id < in.barrier_id) {
        DEBUG("%d: barrier ult: waiting to enter round %d\n", s->rank,
                in.barrier_id);
        aret = ABT_cond_wait(s->barrier_cond, s->barrier_mutex);
        assert(aret == ABT_SUCCESS);
    }

    // inform all other ULTs waiting on this
    aret = ABT_cond_signal(s->barrier_cond);
    assert(aret == ABT_SUCCESS);
    // now wait until all barriers have been rx'd
    DEBUG("%d: barrier ult: out, incr count to %d\n", s->rank,
            s->barrier_count+1);
    s->barrier_count++;
    while (s->barrier_count < s->num_addrs-1) {
        DEBUG("%d: barrier ult: waiting (count at %d)\n", s->rank,
                s->barrier_count);
        aret = ABT_cond_wait(s->barrier_cond, s->barrier_mutex);
        assert(aret == ABT_SUCCESS);
    }
    DEBUG("%d: barrier ult: count compl, signal and respond\n", s->rank);
    // all barriers rx'd, inform other ULTs
    ABT_cond_signal(s->barrier_cond);
    ABT_mutex_unlock(s->barrier_mutex);

    hret = margo_respond(s->mid, h, NULL);
    assert(hret == HG_SUCCESS);
    HG_Destroy(h);

    DEBUG("%d: barrier ult: respond compl, count at %d\n", s->rank,
            s->barrier_count);

    aret = ABT_mutex_lock(s->barrier_mutex);
    assert(aret == ABT_SUCCESS);
    // done -> I'm the last ULT to enter, I do the eventual set
    int is_done = (++s->barrier_count) == 2*(s->num_addrs-1);
    if (is_done) s->barrier_count = 0;
    aret = ABT_mutex_unlock(s->barrier_mutex);
    assert(aret == ABT_SUCCESS);
    if (is_done) {
        aret = ABT_eventual_set(s->barrier_eventual, NULL, 0);
        assert(aret == ABT_SUCCESS);
    }
}

hg_return_t ssg_barrier_margo(ssg_t s)
{
    // non-members can't barrier
    if (s->rank < 0) return HG_INVALID_PARAM;

    // return immediately if a singleton group
    if (s->num_addrs == 1) return HG_SUCCESS;

    int aret = ABT_eventual_reset(s->barrier_eventual);
    if (aret != ABT_SUCCESS) return HG_OTHER_ERROR;

    DEBUG("%d: barrier: lock and incr id to %d\n", s->rank, s->barrier_id+1);
    int bid;
    // init the barrier state
    aret = ABT_mutex_lock(s->barrier_mutex);
    if (aret != ABT_SUCCESS) return HG_OTHER_ERROR;
    bid = ++s->barrier_id;
    aret = ABT_cond_broadcast(s->barrier_cond);
    if (aret != ABT_SUCCESS) {
        ABT_mutex_unlock(s->barrier_mutex); return HG_OTHER_ERROR;
    }
    aret = ABT_mutex_unlock(s->barrier_mutex);
    if (aret != ABT_SUCCESS) return HG_OTHER_ERROR;

    if (s->rank > 0) {
        DEBUG("%d: barrier: create and forward to 0\n", s->rank);
        barrier_in_t in;
        hg_handle_t h;
        hg_return_t hret = HG_Create(margo_get_context(s->mid),
                ssg_get_addr(s, 0), s->barrier_rpc_id, &h);
        if (hret != HG_SUCCESS) return hret;

        in.rank = s->rank;
        in.barrier_id = bid;
        hret = margo_forward(s->mid, h, &in);
        DEBUG("%d: barrier: finish\n", s->rank);
        HG_Destroy(h);
        if (hret != HG_SUCCESS) return hret;
    }
    else {
        DEBUG("%d: barrier: wait on eventual\n", s->rank);
        aret = ABT_eventual_wait(s->barrier_eventual, NULL);
        if (aret != ABT_SUCCESS) return HG_OTHER_ERROR;
    }

    return HG_SUCCESS;
}
#endif

void ssg_finalize(ssg_t s)
{
    if (s == SSG_NULL) return;

#if USE_SWIM_FD
    if(s->swim_ctx)
        swim_finalize(s->swim_ctx);
#endif

#if 0
    if (s->barrier_mutex != ABT_MUTEX_NULL)
        ABT_mutex_free(&s->barrier_mutex);
    if (s->barrier_cond != ABT_COND_NULL)
        ABT_cond_free(&s->barrier_cond);
    if (s->barrier_eventual != ABT_EVENTUAL_NULL)
        ABT_eventual_free(&s->barrier_eventual);
#endif

    for (int i = 0; i < s->view.group_size; i++) {
        if (s->view.member_states[i].addr != HG_ADDR_NULL)
            HG_Addr_free(margo_get_class(s->mid), s->view.member_states[i].addr);
    }
    free(s->view.member_states);
    free(s->addr_str_buf);
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

const char * ssg_get_addr_str(const ssg_t s, int rank)
{
    if (rank >= 0 && rank < s->view.group_size)
        return s->view.member_states[rank].addr_str;
    else
        return NULL;
}

#if 0
// serialization format looks like:
// < num members, buffer size, buffer... >
// doesn't attempt to grab hg_addr's, string buffers, etc. - client will be
// responsible for doing a separate address lookup routine
hg_return_t hg_proc_ssg_t(hg_proc_t proc, ssg_t *s)
{
    // error and return handling
    hg_return_t hret = HG_SUCCESS;
    char * err_str = NULL;

    // input/output vars + helpers for ssg decode setup
    ssg_t ss = NULL;

    switch(hg_proc_get_op(proc)) {
        case HG_ENCODE:
            ss = *s;
            // encode address count
            hret = hg_proc_int32_t(proc, &ss->num_addrs);
            if (hret != HG_SUCCESS) { err_str = "ssg num addrs"; goto end; }
            // encode addr
            hret = hg_proc_int32_t(proc, &ss->buf_size);
            if (hret != HG_SUCCESS) { err_str = "ssg buf size"; goto end; }
            // encode addr string, simple as blitting the backing buffer
            hret = hg_proc_memcpy(proc, ss->backing_buf, ss->buf_size);
            if (hret != HG_SUCCESS) { err_str = "ssg addr buf"; goto end; }
            break;

        case HG_DECODE:
            // create the output
            *s = NULL;
            ss = malloc(sizeof(*ss));
            if (ss == NULL) {
                err_str = "ssg alloc";
                hret = HG_NOMEM_ERROR;
                goto end;
            }
            ss->addr_strs = NULL;
            ss->addrs = NULL;
            ss->backing_buf = NULL;
            // get address count
            hret = hg_proc_int32_t(proc, &ss->num_addrs);
            if (hret != HG_SUCCESS) { err_str = "ssg num addrs"; goto end; }
            // get number of bytes for the address
            hret = hg_proc_int32_t(proc, &ss->buf_size);
            if (hret != HG_SUCCESS) { err_str = "ssg buf size"; goto end; }
            // allocate output buffer
            ss->backing_buf = malloc(ss->buf_size);
            if (hret != HG_SUCCESS) {
                err_str = "ssg buf alloc";
                hret = HG_NOMEM_ERROR;
                goto end;
            }
            hret = hg_proc_memcpy(proc, ss->backing_buf, ss->buf_size);
            if (hret != HG_SUCCESS) { err_str = "ssg addr buf"; goto end; }

            // set the remaining ssg vars

            ss->addr_strs = NULL; ss->addrs = NULL;
            ss->rank = -1; // receivers aren't part of the group

            ss->addr_strs = setup_addr_str_list(ss->num_addrs, ss->backing_buf);
            if (ss->addr_strs == NULL) {
                err_str = "ssg addr strs alloc";
                hret = HG_NOMEM_ERROR;
                goto end;
            }

            ss->addrs = malloc(ss->num_addrs * sizeof(*ss->addrs));
            if (ss->addrs == NULL) {
                err_str = "ssg addrs alloc";
                hret = HG_NOMEM_ERROR;
                goto end;
            }
            for (int i = 0; i < ss->num_addrs; i++) {
                ss->addrs[i] = HG_ADDR_NULL;
            }

            // success: set the output
            *s = ss;
            break;

        case HG_FREE:
            if (s != NULL && *s != NULL) {
                err_str = "ssg shouldn't be freed via HG_Free_*";
                hret = HG_INVALID_PARAM;
            }
            goto end;

        default:
            err_str = "bad proc mode";
            hret = HG_INVALID_PARAM;
    }
end:
    if (err_str) {
        HG_LOG_ERROR("Proc error: %s", err_str);
        if (hg_proc_get_op(proc) == HG_DECODE) {
            free(ss->addr_strs);
            free(ss->addrs);
            free(ss->backing_buf);
            free(ss);
        }
    }
    return hret;
}

int ssg_dump(const ssg_t s, const char *fname)
{
    // file to write to
    int fd = -1;
    ssize_t written;

    // string to xform and dump
    char * addrs_dup = NULL;
    char * tok = NULL;
    char * addrs_dup_end = NULL;

    // return code
    int ret = 0;

    // copy the backing buffer, replacing all null chars with
    // newlines
    addrs_dup = malloc(s->buf_size);
    if (addrs_dup == NULL) { errno = ENOMEM; ret = -1; goto end; }
    memcpy(addrs_dup, s->backing_buf, s->buf_size);
    tok = addrs_dup;
    addrs_dup_end = addrs_dup + s->buf_size;
    for (int i = 0; i < s->num_addrs; i++) {
        tok = memchr(tok, '\0', addrs_dup_end - tok);
        if (tok == NULL) { errno = EINVAL; ret = -1; goto end; }
        *tok = '\n';
    }

    // open the file and dump in a single call
    fd = open(fname, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd == -1) { ret = -1; goto end; }
    // don't include the null char at the end
    written = write(fd, addrs_dup, s->buf_size);
    if (written != s->buf_size) ret = -1;

end:
    free(addrs_dup);
    if (fd != -1) close(fd);

    return ret;
}
#endif

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

#if 0
static uint64_t fnv1a_64(void *data, size_t size)
{
    uint64_t hash = 14695981039346656037ul;
    unsigned char *d = data;

    for (size_t i = 0; i < size; i++) {
        hash ^= (uint64_t)*d++;
        hash *= 1099511628211;
    }
    return hash;
}
#endif

