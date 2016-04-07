#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#include <mercury_proc.h>

#include <ssg.h>
#include <ssg-config.h>
#include "def.h"

#ifdef HAVE_MPI
#include <ssg-mpi.h>
#endif

// helpers for looking up a server
static na_return_t lookup_serv_addr_cb(const struct na_cb_info *info);
static na_addr_t lookup_serv_addr(
        na_class_t *nacl,
        na_context_t *nactx,
        const char *info_str);

static na_return_t find_rank(na_class_t *nacl, ssg_t s);

static char** setup_addr_str_list(int num_addrs, char * buf);

ssg_t ssg_init_config(const char * fname)
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
    s->nacl = NULL;
    s->addrs = malloc(num_addrs*sizeof(*s->addrs));
    if (s->addrs == NULL) goto fini;
    for (int i = 0; i < num_addrs; i++) s->addrs[i] = NA_ADDR_NULL;
    s->addr_strs = addr_strs; addr_strs = NULL;
    s->backing_buf = buf; buf = NULL;
    s->num_addrs = num_addrs;
    s->buf_size = addr_len;
    s->rank = SSG_RANK_UNKNOWN;

fini:
    if (fd != -1) close(fd);
    free(rdbuf);
    free(addr_strs);
    free(buf);
    if (s->addrs == NULL) { free(s); s = NULL; }
    return s;
}

#ifdef HAVE_MPI
ssg_t ssg_init_mpi(na_class_t *nacl, MPI_Comm comm)
{
    // my addr
    na_addr_t self_addr = NA_ADDR_NULL;
    char * self_addr_str = NULL;
    na_size_t self_addr_size = 0;
    int self_addr_size_int = 0; // for mpi-friendly conversion

    // collective helpers
    char * buf = NULL;
    int * sizes = NULL;
    int * sizes_psum = NULL;
    int comm_size = 0;
    int comm_rank = 0;

    // na addresses
    na_addr_t *addrs = NULL;

    // return data
    char **addr_strs = NULL;
    ssg_t s = NULL;

    // misc return codes
    na_return_t nret;

    // get my address
    nret = NA_Addr_self(nacl, &self_addr);
    if (nret != NA_SUCCESS) goto fini;
    nret = NA_Addr_to_string(nacl, NULL, &self_addr_size, self_addr);
    if (self_addr == NULL) goto fini;
    self_addr_str = malloc(self_addr_size);
    if (self_addr_str == NULL) goto fini;
    nret = NA_Addr_to_string(nacl, self_addr_str, &self_addr_size, self_addr);
    if (nret != NA_SUCCESS) goto fini;
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
    for (int i = 0; i < comm_size; i++) addrs[i] = NA_ADDR_NULL;
    addrs[comm_rank] = self_addr;

    // set up the output
    s = malloc(sizeof(*s));
    if (s == NULL) goto fini;
    s->nacl = NULL;
    s->addr_strs = addr_strs; addr_strs = NULL;
    s->addrs = addrs; addrs = NULL;
    s->backing_buf = buf; buf = NULL;
    s->num_addrs = comm_size;
    s->buf_size = sizes_psum[comm_size];
    s->rank = comm_rank;
    self_addr = NA_ADDR_NULL; // don't free this on success

fini:
    if (self_addr != NA_ADDR_NULL) NA_Addr_free(nacl, self_addr);
    free(buf);
    free(sizes);
    free(addr_strs);
    free(addrs);
    return s;
}
#endif

na_return_t ssg_lookup(
        na_class_t *nacl,
        na_context_t *nactx,
        ssg_t s)
{
    // "effective" rank for the lookup loop
    int eff_rank = 0;

    // set the network class up front - will use when destructing
    s->nacl = nacl;

    // perform search for my rank if not already set
    if (s->rank == SSG_RANK_UNKNOWN) {
        na_return_t nret = find_rank(nacl, s);
        if (nret != NA_SUCCESS) return nret;
    }

    if (s->rank == SSG_EXTERNAL_RANK) {
        // do a completely arbitrary effective rank determination to try and
        // prevent everyone talking to the same member at once
        eff_rank = (((intptr_t)nacl)/sizeof(nacl)) % s->num_addrs;
    }
    else
        eff_rank = s->rank;

    // rank is set, perform lookup
    for (int i = eff_rank+1; i < s->num_addrs; i++) {
        s->addrs[i] = lookup_serv_addr(nacl, nactx, s->addr_strs[i]);
        if (s->addrs[i] == NA_ADDR_NULL) return NA_PROTOCOL_ERROR;
    }
    for (int i = 0; i < eff_rank; i++) {
        s->addrs[i] = lookup_serv_addr(nacl, nactx, s->addr_strs[i]);
        if (s->addrs[i] == NA_ADDR_NULL) return NA_PROTOCOL_ERROR;
    }
    if (s->rank == SSG_EXTERNAL_RANK) {
        s->addrs[eff_rank] =
            lookup_serv_addr(nacl, nactx, s->addr_strs[eff_rank]);
        if (s->addrs[eff_rank] == NA_ADDR_NULL) return NA_PROTOCOL_ERROR;
    }

    return NA_SUCCESS;
}

void ssg_finalize(ssg_t s)
{
    for (int i = 0; i < s->num_addrs; i++) {
        if (s->addrs[i] != NA_ADDR_NULL) NA_Addr_free(s->nacl, s->addrs[i]);
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

na_addr_t ssg_get_addr(const ssg_t s, int rank)
{
    if (rank >= 0 || rank < s->num_addrs)
        return s->addrs[rank];
    else
        return NA_ADDR_NULL;
}

const char * ssg_get_addr_str(const ssg_t s, int rank)
{
    if (rank >= 0 || rank < s->num_addrs)
        return s->addr_strs[rank];
    else
        return NULL;
}

// serialization format looks like:
// < num members, buffer size, buffer... >
// doesn't attempt to grab na_addr's, string buffers, etc. - client will be
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
                ss->addrs[i] = NA_ADDR_NULL;
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

typedef struct serv_addr_out
{
    na_addr_t addr;
    int set;
} serv_addr_out_t;

static na_return_t lookup_serv_addr_cb(const struct na_cb_info *info)
{
    serv_addr_out_t *out = info->arg;
    out->addr = info->info.lookup.addr;
    out->set = 1;
    return NA_SUCCESS;
}

static na_addr_t lookup_serv_addr(
        na_class_t *nacl,
        na_context_t *nactx,
        const char *info_str)
{
    serv_addr_out_t out;
    na_return_t nret;

    out.addr = NA_ADDR_NULL;
    out.set = 0;

    nret = NA_Addr_lookup(nacl, nactx, &lookup_serv_addr_cb, &out,
            info_str, NA_OP_ID_IGNORE);
    if (nret != NA_SUCCESS) return NA_ADDR_NULL;

    // run the progress loop until we've got the output
    do {
        unsigned int count = 0;
        do {
            nret = NA_Trigger(nactx, 0, 1, &count);
        } while (nret == NA_SUCCESS && count > 0);

        if (out.set != 0) break;

        nret = NA_Progress(nacl, nactx, 5000);
    } while(nret == NA_SUCCESS || nret == NA_TIMEOUT);

    return out.addr;
}

static na_return_t find_rank(na_class_t *nacl, ssg_t s)
{
    // helpers
    na_addr_t self_addr = NA_ADDR_NULL;
    char * self_addr_str = NULL;
    const char * self_addr_substr = NULL;
    na_size_t self_addr_size = 0;
    const char * addr_substr = NULL;
    int rank = 0;
    na_return_t nret;

    // get my address
    nret = NA_Addr_self(nacl, &self_addr);
    if (nret != NA_SUCCESS) goto end;
    nret = NA_Addr_to_string(nacl, NULL, &self_addr_size, self_addr);
    if (self_addr == NULL) { nret = NA_NOMEM_ERROR; goto end; }
    self_addr_str = malloc(self_addr_size);
    if (self_addr_str == NULL) { nret = NA_NOMEM_ERROR; goto end; }
    nret = NA_Addr_to_string(nacl, self_addr_str, &self_addr_size, self_addr);
    if (nret != NA_SUCCESS) goto end;

    // strstr is used here b/c there may be inconsistencies in whether the class
    // is included in the address or not (it's not in NA_Addr_to_string, it
    // should be in ssg_init_config)
    self_addr_substr = strstr(self_addr_str, "://");
    if (self_addr_substr == NULL) { nret = NA_INVALID_PARAM; goto end; }
    self_addr_substr += 3;
    for (rank = 0; rank < s->num_addrs; rank++) {
        addr_substr = strstr(s->addr_strs[rank], "://");
        if (addr_substr == NULL) { nret = NA_INVALID_PARAM; goto end; }
        addr_substr+= 3;
        if (strcmp(self_addr_substr, addr_substr) == 0)
            break;
    }
    if (rank == s->num_addrs) {
        nret = NA_INVALID_PARAM;
        goto end;
    }

    // success - set
    s->rank = rank;
    s->addrs[rank] = self_addr; self_addr = NULL;

end:
    if (self_addr != NA_ADDR_NULL) NA_Addr_free(nacl, self_addr);
    free(self_addr_str);

    return nret;
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