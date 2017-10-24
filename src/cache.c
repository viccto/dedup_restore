#include <fcntl.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <dirent.h>
#include <string.h>

#include "enums.h"
#include "config.h"
#include "storagemanager.h"
#include "list.h"
#include "metadata.h"
#include "cache.h"

int cache_init(struct cache * cache)
{
	int i;
	if(NULL == cache)
		return -1;
	for(i = 0 ; i < CACHE_LEN ; i ++)
	{
		cache->cache_bucket[i].len = 0;
		list_init(&cache->cache_bucket[i].list);
	}
	return 0;
}

int list_count = 0;

int add_metadata_in_cache(struct metadata * mtdata, size_t len, struct cache * cache)
{
	uint32_t index, pos;
	struct cache_node * cache_node;
	uint32_t i;
	uint32_t first_int;
	struct list * list;

	for(i = 0 ; i < len ; i ++)
	{
		memcpy((void *)&first_int, (void *)mtdata[i].fingerprint, (size_t) sizeof(uint32_t));
		index = first_int & CACHE_MASK;
		pos = cache->cache_bucket[index].len;
		if(pos < CACHE_BUCKET_LEN)
		{
			memcpy(&(cache->cache_bucket[index].cache_node[pos].mtdata), &mtdata[i], sizeof(struct metadata));
			list_add(&cache->cache_bucket[index].list, &cache->cache_bucket[index].cache_node[pos].list);
			cache->cache_bucket[index].len ++;
		}
		else
		{
			list = list_first(&cache->cache_bucket[index].list);
			cache_node = list_item(list, struct cache_node);
			memcpy(&(cache_node->mtdata), &mtdata[i], sizeof(struct metadata));
			list_move(&cache->cache_bucket[index].list, list);
		}
	}
	return 0;
}

int lookup_in_cache(struct cache * cache, char fingerprint[FINGERPRINT_LEN], struct metadata * mtdata)
{
	unsigned long index;
	struct list * list, *head;
	struct cache_node * cache_node;
	uint32_t first_int;

	memcpy(&first_int, fingerprint, sizeof(uint32_t));
	index = first_int & CACHE_MASK;
	head = &cache->cache_bucket[index].list;
	list_uniterate(list, head, head)
	{
		cache_node = list_item(list, struct cache_node);
		if(0 == memcmp(cache_node->mtdata.fingerprint, fingerprint, FINGERPRINT_LEN))
		{
			list_move(head, list);
			memcpy(mtdata, &(cache_node->mtdata), sizeof(struct metadata));
			return 1;
		}
	}
	return 0;
}

