#pragma once

struct ssg
{
    hg_class_t *hgcl;
    char **addr_strs;
    hg_addr_t *addrs;
    void *backing_buf;
    int num_addrs;
    int buf_size;
    int rank;
};
