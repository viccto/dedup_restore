#include <fcntl.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <dirent.h>
#include <string.h>


#include "enums.h"
#include "config.h"
#include "storagemanager.h"
#include "file.h"
#include "disk.h"
#include "diskhash.h"
#include "nodecache.h"
#include "data.h"
#include "container.h"
#include "list.h"
#include "metadata.h"
#include "cache.h"
#include "sha1.h"
#include "bloomfilter.h"
#ifdef COMPRESS
#include "lzjb.h"
#endif
#include "dedup.h"
#include "chunk.h"
#include "optsmr.h"


int disk_hash_init(struct disk_hash * disk_hash)
{
	int i;
	if(NULL == disk_hash)
		return -1;
	for(i = 0 ; i < BUKET_NUM ; i ++)
	{
		disk_hash->hash_bucket[i].seg_stored_len = 0;
		disk_hash->hash_bucket[i].header.previous = 0XFFFFFFFFFFFFFFFF;
		disk_hash->hash_bucket[i].len = 0;
		disk_hash->hash_bucket[i].cur_seg_offset = get_new_seg(disk_hash->manager);
		disk_hash->hash_bucket[i].write_offset = disk_hash->hash_bucket[i].cur_seg_offset;
		simplewrite(disk_hash->hash_bucket[i].write_offset, &disk_hash->hash_bucket[i].header, sizeof(struct disk_hash_seg_header), disk_hash->manager->f);
		disk_hash->hash_bucket[i].write_offset += sizeof(struct disk_hash_seg_header);
	}
	return 0;
}



int add_2_disk_hash(struct disk_hash * disk_hash, struct disk_hash_node *disk_hash_node)
{
#ifdef WRITE_CACHE
    container_log("write to the write cache head:%p\n",GET_FPDD_DATA()->write_cache);
    if(disk_hash_node->zone_id==0){
    	return write_cache_head_add(GET_FPDD_DATA()->write_cache, disk_hash_node);
    }
    else{
        return add_2_disk_hash_table(disk_hash, disk_hash_node);
    }
#else
	return add_2_disk_hash_table(disk_hash, disk_hash_node);
#endif

}

int flush_write_cache(uint64_t zone_id)
{
    container_log("flush the write cache\n");
	struct disk_hash * disk_hash;
	struct node_cache_header *cache_head;
	struct disk_hash_node *disk_hash_node;
	struct dedup_manager * dedup;
	dedup = GET_FPDD_DATA()->dedup;
	disk_hash_node = (struct disk_hash_node*)malloc(sizeof(struct disk_hash_node));
	disk_hash = &dedup->disk_hash;
	cache_head = GET_FPDD_DATA()->write_cache;
	
	pthread_spin_lock(&(cache_head->cache_lock));
	while(write_cache_head_remove_unser(cache_head, disk_hash_node)==1){
		disk_hash_node->zone_id = zone_id;
		add_2_disk_hash_table(disk_hash, disk_hash_node);
	}
	pthread_spin_unlock(&(cache_head->cache_lock));

	free(disk_hash_node);
	return 0;
}
	
	
	


int add_2_disk_hash_table(struct disk_hash * disk_hash, struct disk_hash_node *disk_hash_node)
{
	uint32_t first_int;
	uint32_t index;
	uint32_t left_seg_len;
	uint32_t left_store_len;
	uint32_t store_pos;
	int pos;

	memcpy((void *)&first_int, (void *) (disk_hash_node->fingerprint), sizeof(uint32_t));
	index = first_int & DISK_HASH_MASK;
	if(disk_hash->hash_bucket[index].len >= MEM_HASH_NUM)
	{
		left_store_len = MEM_HASH_NUM;
		store_pos = 0;
		while(left_store_len > 0)
		{
			left_seg_len = DISKHASH_PER_SEG - disk_hash->hash_bucket[index].seg_stored_len;
			if(left_seg_len > left_store_len)
			{
				simplewrite(disk_hash->hash_bucket[index].write_offset, &(disk_hash->hash_bucket[index].disk_hash_node[store_pos]), left_store_len * sizeof(struct disk_hash_node), disk_hash->manager->f);
				disk_hash->hash_bucket[index].len -= left_store_len;
				disk_hash->hash_bucket[index].seg_stored_len += left_store_len;
				disk_hash->hash_bucket[index].write_offset += left_store_len * sizeof(struct disk_hash_node);
				left_store_len -= left_store_len;
				store_pos += left_store_len;
			}
			else
			{
				simplewrite(disk_hash->hash_bucket[index].write_offset, &(disk_hash->hash_bucket[index].disk_hash_node[store_pos]), left_seg_len * sizeof(struct disk_hash_node), disk_hash->manager->f);
				disk_hash->hash_bucket[index].header.previous = disk_hash->hash_bucket[index].cur_seg_offset;
				disk_hash->hash_bucket[index].cur_seg_offset = get_new_seg(disk_hash->manager);
				disk_hash->hash_bucket[index].write_offset = disk_hash->hash_bucket[index].cur_seg_offset;

				simplewrite(disk_hash->hash_bucket[index].write_offset, &disk_hash->hash_bucket[index].header, sizeof(struct disk_hash_seg_header), disk_hash->manager->f);
				disk_hash->hash_bucket[index].write_offset += sizeof(struct disk_hash_seg_header);

				disk_hash->hash_bucket[index].len -= left_seg_len;
				disk_hash->hash_bucket[index].seg_stored_len = 0;

				left_store_len -= left_seg_len;
				store_pos += left_seg_len;
			}
		}
	}
	pos = disk_hash->hash_bucket[index].len;
	memcpy(&(disk_hash->hash_bucket[index].disk_hash_node[pos]), disk_hash_node, sizeof(struct disk_hash_node));
	disk_hash->hash_bucket[index].len ++;
	return 0;
}




int lookup_fingerprint_in_disk_hash(struct disk_hash * disk_hash, char fingerprint[FINGERPRINT_LEN], struct disk_hash_node * disk_hash_node)
{
	container_log("enter lookup_fingerprint_in_disk_hash\n");
	uint32_t first_int;
	uint32_t index;
	size_t len;
	uint32_t read_len;
	uint64_t read_offset;
	struct disk_hash_seg_header * header;
	struct disk_hash_node * node;
	struct disk_hash_node * cache_node;
	int i;
	memcpy((void *)&first_int, (void *)fingerprint, sizeof(uint32_t));
	index = first_int & DISK_HASH_MASK;
	len = disk_hash->hash_bucket[index].len;

#ifdef WRITE_CACHE
	cache_node = write_cache_lookup(GET_FPDD_DATA()->write_cache, fingerprint);
    if(cache_node!=NULL)
    {
        memcpy(disk_hash_node, cache_node, sizeof(struct disk_hash_node));
	return 1;
    }
#endif


	for(i = 0 ; i < len ; i ++)
	{
		if(0 == memcmp(disk_hash->hash_bucket[index].disk_hash_node[i].fingerprint, fingerprint, FINGERPRINT_LEN))
		{
			memcpy(disk_hash_node, &(disk_hash->hash_bucket[index].disk_hash_node[i]), sizeof(struct disk_hash_node));
			return 1;
		}
	}
	if(disk_hash->hash_bucket[index].seg_stored_len > 0)
	{
		read_len = sizeof(struct disk_hash_node) * disk_hash->hash_bucket[index].seg_stored_len + sizeof(struct disk_hash_seg_header);
		read_offset = disk_hash->hash_bucket[index].cur_seg_offset;
	}
	else
	{
		read_len = SEG_SIZE;
		read_offset = disk_hash->hash_bucket[index].header.previous;
	}
	while(0XFFFFFFFFFFFFFFFF != read_offset)
	{
		simpleread(read_offset, disk_hash->read_seg, read_len, disk_hash->manager->f);
		if(SEG_SIZE == read_len)
		{
			len = DISKHASH_PER_SEG;
		}
		else
		{
			len = disk_hash->hash_bucket[index].seg_stored_len;
		}
		node = (struct disk_hash_node *)(disk_hash->read_seg + sizeof(struct disk_hash_seg_header));
		for(i = 0 ; i < len ; i ++)
		{
			if(0 == memcmp(node[i].fingerprint, fingerprint, FINGERPRINT_LEN))
			{
				memcpy(disk_hash_node, &node[i], sizeof(struct disk_hash_node));
				return 1;
			}
		}
		read_len = SEG_SIZE;
		header = (struct disk_hash_seg_header *)disk_hash->read_seg;
		read_offset = header->previous;
	}
	return 0;
}




struct disk_hash_node * lookup_fingerprint_in_disk_hash_ptr(struct disk_hash * disk_hash, char fingerprint[FINGERPRINT_LEN], int * file)
{
	container_log("enter lookup_fingerprint_in_disk_hash_ptr\n");
	struct disk_hash_node * chunk_node;
	uint32_t first_int;
	uint32_t index;
	size_t len;
	uint32_t read_len;
	uint64_t read_offset;
	struct disk_hash_seg_header * header;
	struct disk_hash_node * node;
	int i;
	memcpy((void *)&first_int, (void *)fingerprint, sizeof(uint32_t));
	index = first_int & DISK_HASH_MASK;
	len = disk_hash->hash_bucket[index].len;
    chunk_node = NULL;

#ifdef WRITE_CACHE
	chunk_node = write_cache_lookup(GET_FPDD_DATA()->write_cache, fingerprint);
	if(chunk_node!=NULL){
        container_log("in the write cache\n");
		*file = 0;
		return chunk_node;
	}
        container_log("not in the write cache, look at the dedup table\n");
#endif

	for(i = 0 ; i < len ; i ++)
	{
		if((0 == memcmp(disk_hash->hash_bucket[index].disk_hash_node[i].fingerprint, fingerprint, FINGERPRINT_LEN))&&(disk_hash->hash_bucket[index].disk_hash_node[i].deleted==false))
		{
			*file = 0;
			return &(disk_hash->hash_bucket[index].disk_hash_node[i]);
		}
	}
	if(disk_hash->hash_bucket[index].seg_stored_len > 0)
	{
		read_len = sizeof(struct disk_hash_node) * disk_hash->hash_bucket[index].seg_stored_len + sizeof(struct disk_hash_seg_header);
		read_offset = disk_hash->hash_bucket[index].cur_seg_offset;
	}
	else
	{
		read_len = SEG_SIZE;
		read_offset = disk_hash->hash_bucket[index].header.previous;
	}
	while(0XFFFFFFFFFFFFFFFF != read_offset)
	{
		simpleread(read_offset, disk_hash->read_seg, read_len, disk_hash->manager->f);
		if(SEG_SIZE == read_len)
		{
			len = DISKHASH_PER_SEG;
		}
		else
		{
			len = disk_hash->hash_bucket[index].seg_stored_len;
		}
		node = (struct disk_hash_node *)(disk_hash->read_seg + sizeof(struct disk_hash_seg_header));
		for(i = 0 ; i < len ; i ++)
		{
			if((0 == memcmp(node[i].fingerprint, fingerprint, FINGERPRINT_LEN))&&(node[i].deleted==false))
			{
				chunk_node = (struct disk_hash_node *)malloc(sizeof(struct disk_hash_node));
				memcpy(chunk_node, &node[i], sizeof(struct disk_hash_node));
				*file = 1;
				return chunk_node;
			}
		}
		read_len = SEG_SIZE;
		header = (struct disk_hash_seg_header *)disk_hash->read_seg;
		read_offset = header->previous;
	}
	return NULL;
}

