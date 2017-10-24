/*
 * chunk_cache.h
 * the cache for restore
 * author: zhichao cao
 * date: 2017/02/17
 */

#ifndef _CHUNK_CACHE_H_
#define _CHUNK_CACHE_H_

struct _chunk_cache_node
{
	uint64_t chunk_name;
	int chunk_state; /*p-chunk is 0, e-chunk is 1*/
	char *chunk_ptr;
	struct _chunk_cache_node *lru_pre_ptr; /*pointer for lru*/
	struct _chunk_cache_node *lru_next_ptr; /*pointer for lru*/
	struct _chunk_cache_node *hm_pre_ptr; /*pointer for hashmap*/
	struct _chunk_cache_node *hm_next_ptr; /*pointer for hashmap*/
};

struct _chunk_cache_table
{
	uint32_t CHUNK_CACHE_NUM;
	uint32_t cached_num;
	uint32_t e_num;
	uint32_t p_num;
	struct _chunk_cache_node table[CHUNK_CACHE_BUCK];
	struct _chunk_cache_node lru;	/*the lru chain head*/
	struct _chunk_cache_node s_ptr; /*the separete point in lru chain*/
};

bool s_ptr_not_at_tail(void);

struct _chunk_cache_table *chunk_cache_init(void);

void chunk_cache_destroy(struct _chunk_cache_table *chunk_cache);

void chunk_cache_remove_node(struct _chunk_cache_node *con_node);

uint64_t chunk_cache_evict(void);

int add_2_chunk_cache(char *chunk_ptr, uint64_t chunk_name);

struct _chunk_cache_node * find_chunk_cache(uint64_t chunk_name);

void chunk_cache_move_mru(struct _chunk_cache_node *tmp);

void add_2_chunk_cache_head(struct _chunk_cache_node *tmp);

void add_2_chunk_cache_after_s_prt(struct _chunk_cache_node *tmp);

void add_2_chunk_cache_before_s_prt(struct _chunk_cache_node *tmp);

void move_2_chunk_cache_head(struct _chunk_cache_node *tmp);

void move_2_chunk_cache_after_s_prt(struct _chunk_cache_node *tmp);

void move_2_chunk_cache_before_s_prt(struct _chunk_cache_node *tmp);

int chunk_cache_pcache_size(void);







#endif
