/*
 * enums.h - This file contains the definition of all the enums
 * creat date: 2017/02/03
 * author: Zhichao Cao
 */
#ifndef ENUMS_H
#define ENUMS_H

#include <stdbool.h>
#include <stdint.h>

enum _open_mode {
    CREATE = 0,
    READ   = 1,
    WRITE  = 2,
    DIRTY  = 3,
};

enum _list_type {
    UNKNOWN_LIST     = 0,
    DOBJ_DIRTY_LIST  = 1,
    LNODE_DIRTY_LIST = 2,
    BNODE_DIRTY_LIST = 3,
    SNAPSHOT_LIST    = 4,
    RECYCLE_LIST     = 5
};

enum _node_type {
    UNKNOWN_NODE = 0,
    BNODE        = 1,
    LNODE        = 2
};
#define NUM_TREES LNODE

enum _tree_type {
    UNKNOWN_TREE = 0,
    META_TREE    = 1,
    DATA_TREE    = 2
};

enum _dobj_type {
    UNKNOWN_DOBJ  = 0,
    FREE_DOBJ     = 1,
    INODE_DOBJ    = 2,
    DIRENT_DOBJ   = 3,
    DATA_DOBJ     = 4,
    INDIRECT_DOBJ = 5,
    ACL_DOBJ      = 6,
    LINK_DOBJ     = 7,
};

enum _h_index {
    L_OFFSET       = 0,
    H1_INDEX       = 1,
    H2_INDEX       = 2,
    H3_INDEX       = 3,
    UNKNOWN_HEIGHT = -1
};


enum _hashmap_id {
    MTREE_BNODE_CACHE_ID     = 0,
    MTREE_LNODE_CACHE_ID     = 1,
    MTREE_DOBJ_CACHE_ID      = 2,
    MTREE_FREE_DOBJ_CACHE_ID = 3,
    DTREE_BNODE_CACHE_ID     = 4,
    DTREE_LNODE_CACHE_ID     = 5,
    DTREE_DOBJ_CACHE_ID      = 6,
    DTREE_FREE_DOBJ_CACHE_ID = 7,
    SNAP_BNODE_CACHE_ID      = 8,
    SNAP_LNODE_CACHE_ID      = 9,
    LAST_CACHE_ID            = 10
};

enum _iter_state {
   ITER_VALID = 0,
   ITER_INVALID,
};

enum {
    LOG_LVL_ENTER = 0x00000001,
    LOG_LVL_EXIT  = 0x00000002,
    LOG_LVL_ERROR = 0x00000004,
    LOG_LVL_WARN  = 0x00000008,
    LOG_LVL_INFO  = 0x00000010,
    LOG_LVL_DEBUG = 0x00000020,
    LOG_LVL_REF   = 0x00000040,
    LOG_LVL_ALL   = 0xFFFFFFFF,
};

enum {
    DBG_SUPER  = 0x00000001,
    DBG_VFS    = 0x00000002,
    DBG_TREE   = 0x00000004,
    DBG_BNODE  = 0x00000008,
    DBG_LNODE  = 0x00000010,
    DBG_DOBJ   = 0x00000020,
    DBG_INODE  = 0x00000040,
    DBG_DIRENT = 0x00000080,
    DBG_FILE   = 0x00000100,
    DBG_IOCTL  = 0x00000200,
    DBG_CLOUD  = 0x00000400,
    DBG_SNAP   = 0x00000800,
    DBG_ALL    = 0xFFFFFFFF,
};


#endif // ENUMS_H
