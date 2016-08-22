/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include <mercury_proc.h>

#include <ssg.h>
#include <ssg-config.h>
#include "def.h"

#ifdef HAVE_MPI
#include <ssg-mpi.h>
#endif
#ifdef HAVE_MARGO
#include <ssg-margo.h>
#endif

// helpers for looking up a server
static hg_return_t lookup_serv_addr_cb(const struct hg_cb_info *info);
static hg_addr_t lookup_serv_addr(
        hg_context_t *hgctx,
        const char *info_str);

static hg_return_t find_rank(hg_class_t *hgcl, ssg_t s);

static char** setup_addr_str_list(int num_addrs, char * buf);

ssg_t ssg_init_config(const char * fname, int is_member)
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
    void *buf = NULL;
    char **addr_strs = NULL;

    // return var
    ssg_t s = NULL;

    // misc return codes
    int ret;

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
    if (rdsz <= 0) goto fini;
    if (rdsz != st.st_size) { free(rdbuf); close(fd); return NULL; }
    rdbuf[rdsz]='\0';

    // strtok the result - each space-delimited address is assumed to be
    // a unique mercury address
    tok = strtok(rdbuf, "\r\n\t ");
    if (tok == NULL) goto fini;

    // build up the address buffer
    buf = malloc(addr_cap);
    if (buf == NULL) goto fini;
    do {
        int tok_sz = strlen(tok);
        if (tok_sz + addr_len + 1 > addr_cap) {
            void * tmp;
            addr_cap *= 2;
            tmp = realloc(buf, addr_cap);
            if (tmp == NULL) goto fini;
            buf = tmp;
        }
        memcpy((char*)buf + addr_len, tok, tok_sz+1);
        addr_len += tok_sz+1;
        num_addrs++;
        tok = strtok(NULL, "\r\n\t ");
    } while (tok != NULL);

    // set up the list of addresses
    addr_strs = malloc(num_addrs * sizeof(*addr_strs));
    if (addr_strs == NULL) goto fini;
    tok = (char*)buf;
    for (int i = 0; i < num_addrs; i++) {
        addr_strs[i] = tok;
        tok += strlen(tok) + 1;
    }

    // done parsing - setup the return structure
    s = malloc(sizeof(*s));
    if (s == NULL) goto fini;
    s->hgcl = NULL;
    s->addrs = malloc(num_addrs*sizeof(*s->addrs));
    if (s->addrs == NULL) goto fini;
    for (int i = 0; i < num_addrs; i++) s->addrs[i] = HG_ADDR_NULL;
    s->addr_strs = addr_strs; addr_strs = NULL;
    s->backing_buf = buf; buf = NULL;
    s->num_addrs = num_addrs;
    s->buf_size = addr_len;
    s->rank = is_member ? SSG_RANK_UNKNOWN : SSG_EXTERNAL_RANK;

fini:
    if (fd != -1) close(fd);
    free(rdbuf);
    free(addr_strs);
    free(buf);
    if (s != NULL && s->addrs == NULL) { free(s); s = NULL; }
    return s;
}

#ifdef HAVE_MPI
ssg_t ssg_init_mpi(hg_class_t *hgcl, MPI_Comm comm)
{
    // my addr
    hg_addr_t self_addr = HG_ADDR_NULL;
    char * self_addr_str = NULL;
    hg_size_t self_addr_size = 0;
    int self_addr_size_int = 0; // for mpi-friendly conversion

    // collective helpers
    char * buf = NULL;
    int * sizes = NULL;
    int * sizes_psum = NULL;
    int comm_size = 0;
    int comm_rank = 0;

    // hg addresses
    hg_addr_t *addrs = NULL;

    // return data
    char **addr_strs = NULL;
    ssg_t s = NULL;

    // misc return codes
    hg_return_t hret;

    // get my address
    hret = HG_Addr_self(hgcl, &self_addr);
    if (hret != HG_SUCCESS) goto fini;
    hret = HG_Addr_to_string(hgcl, NULL, &self_addr_size, self_addr);
    if (self_addr == NULL) goto fini;
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
    buf = malloc(sizes_psum[comm_size]);
    if (buf == NULL) goto fini;
    MPI_Allgatherv(self_addr_str, self_addr_size_int, MPI_BYTE,
            buf, sizes, sizes_psum, MPI_BYTE, comm);

    // set the addresses
    addr_strs = setup_addr_str_list(comm_size, buf);
    if (addr_strs == NULL) goto fini;

    // init peer addresses
    addrs = malloc(comm_size*sizeof(*addrs));
    if (addrs == NULL) goto fini;
    for (int i = 0; i < comm_size; i++) addrs[i] = HG_ADDR_NULL;
    addrs[comm_rank] = self_addr;

    // set up the output
    s = malloc(sizeof(*s));
    if (s == NULL) goto fini;
    s->hgcl = NULL; // set in ssg_lookup
    s->addr_strs = addr_strs; addr_strs = NULL;
    s->addrs = addrs; addrs = NULL;
    s->backing_buf = buf; buf = NULL;
    s->num_addrs = comm_size;
    s->buf_size = sizes_psum[comm_size];
    s->rank = comm_rank;
    self_addr = HG_ADDR_NULL; // don't free this on success

fini:
    if (self_addr != HG_ADDR_NULL) HG_Addr_free(hgcl, self_addr);
    free(buf);
    free(sizes);
    free(addr_strs);
    free(addrs);
    return s;
}
#endif

hg_return_t ssg_lookup(ssg_t s, hg_context_t *hgctx)
{
    // "effective" rank for the lookup loop
    int eff_rank = 0;

    // set the hg class up front - need for destructing addrs
    s->hgcl = HG_Context_get_class(hgctx);
    if (s->hgcl == NULL) return HG_INVALID_PARAM;

    // perform search for my rank if not already set
    if (s->rank == SSG_RANK_UNKNOWN) {
        hg_return_t hret = find_rank(s->hgcl, s);
        if (hret != HG_SUCCESS) return hret;
    }

    if (s->rank == SSG_EXTERNAL_RANK) {
        // do a completely arbitrary effective rank determination to try and
        // prevent everyone talking to the same member at once
        eff_rank = (((intptr_t)hgctx)/sizeof(hgctx)) % s->num_addrs;
    }
    else
        eff_rank = s->rank;

    // rank is set, perform lookup
    for (int i = eff_rank+1; i < s->num_addrs; i++) {
        s->addrs[i] = lookup_serv_addr(hgctx, s->addr_strs[i]);
        if (s->addrs[i] == HG_ADDR_NULL) return HG_PROTOCOL_ERROR;
    }
    for (int i = 0; i < eff_rank; i++) {
        s->addrs[i] = lookup_serv_addr(hgctx, s->addr_strs[i]);
        if (s->addrs[i] == HG_ADDR_NULL) return HG_PROTOCOL_ERROR;
    }
    if (s->rank == SSG_EXTERNAL_RANK) {
        s->addrs[eff_rank] =
            lookup_serv_addr(hgctx, s->addr_strs[eff_rank]);
        if (s->addrs[eff_rank] == HG_ADDR_NULL) return HG_PROTOCOL_ERROR;
    }

    return HG_SUCCESS;
}

#ifdef HAVE_MARGO

struct lookup_ult_args
{
    ssg_t ssg;
    margo_instance_id mid;
    int rank;
    hg_return_t out;
};

static void lookup_ult(void *arg)
{
    struct lookup_ult_args *l = arg;

    l->out = margo_addr_lookup(l->mid, l->ssg->addr_strs[l->rank],
            &l->ssg->addrs[l->rank]);
}

// TODO: refactor - code is mostly a copy of ssg_lookup
hg_return_t ssg_lookup_margo(ssg_t s, margo_instance_id mid)
{
    hg_context_t *hgctx;
    ABT_thread *ults;
    struct lookup_ult_args *args;
    hg_return_t hret = HG_SUCCESS;

    // "effective" rank for the lookup loop
    int eff_rank = 0;

    // set the hg class up front - need for destructing addrs
    hgctx = margo_get_context(mid);
    if (hgctx == NULL) return HG_INVALID_PARAM;
    s->hgcl = margo_get_class(mid);
    if (s->hgcl == NULL) return HG_INVALID_PARAM;

    // perform search for my rank if not already set
    if (s->rank == SSG_RANK_UNKNOWN) {
        hret = find_rank(s->hgcl, s);
        if (hret != HG_SUCCESS) return hret;
    }

    if (s->rank == SSG_EXTERNAL_RANK) {
        // do a completely arbitrary effective rank determination to try and
        // prevent everyone talking to the same member at once
        eff_rank = (((intptr_t)hgctx)/sizeof(hgctx)) % s->num_addrs;
    }
    else
        eff_rank = s->rank;

    // initialize ULTs
    ults = malloc(s->num_addrs * sizeof(*ults));
    if (ults == NULL) return HG_NOMEM_ERROR;
    args = malloc(s->num_addrs * sizeof(*args));
    if (args == NULL) {
        free(ults);
        return HG_NOMEM_ERROR;
    }
    for (int i = 0; i < s->num_addrs; i++)
        ults[i] = ABT_THREAD_NULL;

    for (int i = eff_rank+1; i < s->num_addrs; i++) {
        args[i].ssg = s;
        args[i].mid = mid;
        args[i].rank = i;

        int aret = ABT_thread_create(*margo_get_handler_pool(mid), &lookup_ult,
                &args[i], ABT_THREAD_ATTR_NULL, &ults[i]);
        if (aret != ABT_SUCCESS) {
            hret = HG_OTHER_ERROR;
            goto fin;
        }
    }
    for (int i = 0; i < eff_rank; i++) {
        args[i].ssg = s;
        args[i].mid = mid;
        args[i].rank = i;

        int aret = ABT_thread_create(*margo_get_handler_pool(mid), &lookup_ult,
                &args[i], ABT_THREAD_ATTR_NULL, &ults[i]);
        if (aret != ABT_SUCCESS) {
            hret = HG_OTHER_ERROR;
            goto fin;
        }
    }
    if (s->rank == SSG_EXTERNAL_RANK) {
        args[eff_rank].ssg = s;
        args[eff_rank].mid = mid;
        args[eff_rank].rank = eff_rank;

        int aret = ABT_thread_create(*margo_get_handler_pool(mid), &lookup_ult,
                &args[eff_rank], ABT_THREAD_ATTR_NULL, &ults[eff_rank]);
        if (aret != ABT_SUCCESS) {
            hret = HG_OTHER_ERROR;
            goto fin;
        }
    }

    // wait on all
    for (int i = 0; i < s->num_addrs; i++) {
        if (ults[i] != ABT_THREAD_NULL) {
            int aret = ABT_thread_join(ults[i]);
            ABT_thread_free(&ults[i]);
            ults[i] = ABT_THREAD_NULL; // in case of cascading failure from join
            if (aret != ABT_SUCCESS) {
                hret = HG_OTHER_ERROR;
                break;
            }
            else if (args[i].out != HG_SUCCESS) {
                hret = args[i].out;
                break;
            }
        }
    }

fin:
    // cleanup
    if (ults != NULL) {
        for (int i = 0; i < s->num_addrs; i++) {
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
#endif

void ssg_finalize(ssg_t s)
{
    if (s == SSG_NULL) return;

    for (int i = 0; i < s->num_addrs; i++) {
        if (s->addrs[i] != HG_ADDR_NULL) HG_Addr_free(s->hgcl, s->addrs[i]);
    }
    free(s->backing_buf);
    free(s->addr_strs);
    free(s->addrs);
    free(s);
}

int ssg_get_rank(const ssg_t s)
{
    return s->rank;
}

int ssg_get_count(const ssg_t s)
{
    return s->num_addrs;
}

hg_addr_t ssg_get_addr(const ssg_t s, int rank)
{
    if (rank >= 0 && rank < s->num_addrs)
        return s->addrs[rank];
    else
        return HG_ADDR_NULL;
}

const char * ssg_get_addr_str(const ssg_t s, int rank)
{
    if (rank >= 0 && rank < s->num_addrs)
        return s->addr_strs[rank];
    else
        return NULL;
}

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

typedef struct serv_addr_out
{
    hg_addr_t addr;
    int set;
} serv_addr_out_t;

static hg_return_t lookup_serv_addr_cb(const struct hg_cb_info *info)
{
    serv_addr_out_t *out = info->arg;
    out->addr = info->info.lookup.addr;
    out->set = 1;
    return HG_SUCCESS;
}

static hg_addr_t lookup_serv_addr(
        hg_context_t *hgctx,
        const char *info_str)
{
    serv_addr_out_t out;
    hg_return_t hret;

    out.addr = HG_ADDR_NULL;
    out.set = 0;

    hret = HG_Addr_lookup(hgctx, &lookup_serv_addr_cb, &out,
            info_str, HG_OP_ID_IGNORE);
    if (hret != HG_SUCCESS) return HG_ADDR_NULL;

    // run the progress loop until we've got the output
    do {
        unsigned int count = 0;
        do {
            hret = HG_Trigger(hgctx, 0, 1, &count);
        } while (hret == HG_SUCCESS && count > 0);

        if (out.set != 0) break;

        hret = HG_Progress(hgctx, 5000);
    } while(hret == HG_SUCCESS || hret == HG_TIMEOUT);

    return out.addr;
}

static hg_return_t find_rank(hg_class_t *hgcl, ssg_t s)
{
    // helpers
    hg_addr_t self_addr = HG_ADDR_NULL;
    char * self_addr_str = NULL;
    const char * self_addr_substr = NULL;
    hg_size_t self_addr_size = 0;
    const char * addr_substr = NULL;
    int rank = 0;
    hg_return_t hret;

    // get my address
    hret = HG_Addr_self(hgcl, &self_addr);
    if (hret != HG_SUCCESS) goto end;
    hret = HG_Addr_to_string(hgcl, NULL, &self_addr_size, self_addr);
    if (self_addr == NULL) { hret = HG_NOMEM_ERROR; goto end; }
    self_addr_str = malloc(self_addr_size);
    if (self_addr_str == NULL) { hret = HG_NOMEM_ERROR; goto end; }
    hret = HG_Addr_to_string(hgcl, self_addr_str, &self_addr_size, self_addr);
    if (hret != HG_SUCCESS) goto end;

    // strstr is used here b/c there may be inconsistencies in whether the class
    // is included in the address or not (it's not in HG_Addr_to_string, it
    // should be in ssg_init_config)
    self_addr_substr = strstr(self_addr_str, "://");
    if (self_addr_substr == NULL) { hret = HG_INVALID_PARAM; goto end; }
    self_addr_substr += 3;
    for (rank = 0; rank < s->num_addrs; rank++) {
        addr_substr = strstr(s->addr_strs[rank], "://");
        if (addr_substr == NULL) { hret = HG_INVALID_PARAM; goto end; }
        addr_substr+= 3;
        if (strcmp(self_addr_substr, addr_substr) == 0)
            break;
    }
    if (rank == s->num_addrs) {
        hret = HG_INVALID_PARAM;
        goto end;
    }

    // success - set
    s->rank = rank;
    s->addrs[rank] = self_addr; self_addr = NULL;

end:
    if (self_addr != HG_ADDR_NULL) HG_Addr_free(hgcl, self_addr);
    free(self_addr_str);

    return hret;
}

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
