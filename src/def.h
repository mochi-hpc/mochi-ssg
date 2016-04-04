#pragma once

struct ssg
{
    na_class_t *nacl;
    char **addr_strs;
    na_addr_t *addrs;
    void *backing_buf;
    int num_addrs;
    int buf_size;
    int rank;
};
