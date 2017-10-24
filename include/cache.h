/*
 * cache.h
 *
 *  Created on: 2011-7-19
 *      Author: badboy
 */

#ifndef CACHE_H_
#define CACHE_H_



struct cache_node
{
	struct list list;
	struct metadata mtdata;
};

struct cache_bucket
{
	uint32_t len;
	struct list list;
	struct cache_node cache_node[CACHE_BUCKET_LEN];
};

struct cache
{
	struct cache_bucket cache_bucket[CACHE_LEN];
};

int cache_init(struct cache * cache);

int add_metadata_in_cache(struct metadata * mtdata, size_t len, struct cache * cache);

int lookup_in_cache(struct cache * cache, char fingerprint[FINGERPRINT_LEN], struct metadata *mtdata);


#endif /* CACHE_H_ */
