/*
 * nodecache.h
 *
 *  Created on: 2016-11-30
 *      Author: zhichao cao
 */

#ifndef NODE_CACHE_H_
#define NODE_CACHE_H_

struct node_cache
{
	struct disk_hash_node *next;
	struct disk_hash_node node;
};

struct node_cache_header
{
	pthread_spinlock_t cache_lock;
	struct disk_hash_node *next;
	uint64_t size;
};

struct node_cache_header *write_cache_init();

int write_cache_head_add(struct node_cache_header *cache_head, struct disk_hash_node *node);

int write_cache_head_remove(struct node_cache_header *cache_head, struct disk_hash_node *node);

int write_cache_head_remove_unser(struct node_cache_header *cache_head, struct disk_hash_node *node);

struct disk_hash_node * write_cache_lookup(struct node_cache_header *cache_head, char fingerprint[FINGERPRINT_LEN]);

int write_cache_destroy(struct node_cache_header *cache_head);

	
#endif

