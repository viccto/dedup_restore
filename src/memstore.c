/* 
 * memstore.c, it is the new verson of indexing table and caching designs for DRAM, NVM and SSD
 * author: zhichao cao
 * created date: 04/18/2017
 */

#include<limits.h>
#include<stdarg.h>
#include<stdlib.h>
#include<stdio.h>
#include<string.h>
#include<stdarg.h>

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
#include "memstore.h"
#ifdef COMPRESS
#include "lzjb.h"
#endif
#include "dedup.h"
#include "chunk.h"
#include "optsmr.h"


uint32_t bkdr_hash(char *str)
{

	uint32_t seed = 131;
	uint32_t hash = 0;
 
	while (*str)
	{
		hash = hash * seed + (*str++);
	}
 
	return (hash & 0x7FFFFFFF);

/*
	uint32_t h=0;
    while(*str) h=(uint32_t)*str++ + (h<<6) + (h<<16) - h;
    return h;
*/
}

/*create a new empty mem_seg, but not initiated*/
struct mem_seg *mem_seg_create(void)
{
	struct mem_seg * tmp_seg;
	tmp_seg = (struct mem_seg *)malloc(sizeof(struct mem_seg));
	if(tmp_seg==NULL)
		return NULL;
	else
		return tmp_seg;
}


/*if the seg is just created, init it,
*/
int mem_seg_init(struct mem_seg *seg, struct memstore *store, uint32_t seg_hash_id)
{
	if((seg==NULL)||(store==NULL))
		return -1;

	int i;	

	seg->seg_hash_id = seg_hash_id;
	seg->bf_id = store->bf_count;
	store->bf_count++;
	seg->dirty = 0;
	seg->current_ptr = 0;
	seg->cache_level = 1;
	seg->bloom = bloom_init();
	seg->node_map = (struct mem_node**)malloc(MEMSTORE_NODE_BUCK*sizeof(struct mem_node *));
	for(i=0;i<MEMSTORE_NODE_BUCK;i++)
	{
		seg->node_map[i]=NULL;
	}
	seg->node_array = (struct mem_node *)malloc(MEMSTORE_NODE_NUM*sizeof(struct mem_node));
	seg->hash_next = seg;
	seg->hash_pre = seg;
	seg->lru_next = NULL;
	seg->lru_pre = NULL;
	
	return 0;
}


/*if the mem_seg already been used, just reload it
 * so do a new one*/
int mem_seg_new(struct mem_seg *seg, struct memstore *store)
{
	if((seg==NULL)||(store==NULL))
		return -1;

	int i;	

	seg->bf_id = store->bf_count;
	store->bf_count++;
	seg->dirty = 0;
	seg->current_ptr = 0;
	seg->cache_level = 1;
	seg->bloom = bloom_init();
	seg->node_map = (struct mem_node**)malloc(MEMSTORE_NODE_BUCK*sizeof(struct mem_node *));
	for(i=0;i<MEMSTORE_NODE_BUCK;i++)
	{
		seg->node_map[i]=NULL;
	}
	seg->node_array = (struct mem_node *)malloc(MEMSTORE_NODE_NUM*sizeof(struct mem_node));
	
	return 0;
}

/*destroy the mem seg free the sapce*/
void mem_seg_destroy(struct mem_seg *seg)
{
	if(seg==NULL)
		return;
	if(seg->node_map!=NULL)
		free(seg->node_map);
	if(seg->node_array!=NULL)
		free(seg->node_array);
	if(seg->bloom!=NULL)
		bloom_destroy(seg->bloom);
	return;
}


/*initiate a new memstore, called when system is begin*/
struct memstore *memstore_init(void)
{
	int i;
	struct memstore *store;
	store = (struct memstore *)malloc(sizeof(struct memstore));
	
	store->bf_count = 0;
	store->seg_cache_num = 0;
	store->bf_cache_num = 0;
	store->seg_total_num = 0;
	store->node_num = 0;
	store->node_cache_hit_count = 0;
	for(i=0;i<MEMSTORE_SEG_BUCK;i++)
	{
		mem_seg_init(&(store->seg_map[i]), store, i);
	}
	store->seg_lru.lru_next = &(store->seg_lru);
	store->seg_lru.lru_pre = &(store->seg_lru);

	store->bf_lru.lru_next = &(store->bf_lru);
	store->bf_lru.lru_pre = &(store->bf_lru);

	store->null_list.lru_next = &(store->null_list);
	store->null_list.lru_pre = &(store->null_list);

	/*initiate the l0 cache*/
	for(i=0;i<MEMSTORE_NODE_CACHE_BUCK;i++)
	{
		store->node_cache[i].next = NULL;
		store->node_cache[i].lru_next = NULL;
		store->node_cache[i].lru_pre = NULL;
	}
	store->node_lru.lru_next = &(store->node_lru);
	store->node_lru.lru_pre = &(store->node_lru);

	return store;
}


/*remove the seg from the hash tabel and LRU list*/
void mem_seg_remove(struct mem_seg *seg)
{
	if(seg==NULL)
		return;
	
	seg->hash_pre->hash_next  = seg->hash_next;
	seg->hash_next->hash_pre = seg->hash_pre;

	seg->lru_pre->lru_next  = seg->lru_next;
	seg->lru_next->lru_pre = seg->lru_pre;

	return;	
}


/*destroy the memstore, free the memory, called when the system is down*/
void memstore_destroy(struct memstore *store)
{
	int i;
	struct mem_seg *seg;
	
	for(i=0;i<MEMSTORE_SEG_BUCK;i++)
	{
		seg = store->seg_map[i].hash_next;
		
		while(seg!=&(store->seg_map[i]))
		{
			mem_seg_remove(seg);
			mem_seg_destroy(seg);
			free(seg);
			seg = store->seg_map[i].hash_next;
		}
		mem_seg_destroy(&(store->seg_map[i]));
	}
	
	free(store);
	return;
}



/*add one chunk meta to the memstore*/
int memstore_add(struct memstore *store, struct disk_hash_node *disk_hash_node)
{
	//memstore_log("memstore_add.....\n");
	if(store==NULL||disk_hash_node==NULL)
		return -1;
	
	uint32_t bkdrhash;
	int seg_buck, node_buck;
	struct mem_seg *seg, *new_seg;
	struct mem_node *node;

	bkdrhash = bkdr_hash(disk_hash_node->fingerprint);
	seg_buck = bkdrhash%MEMSTORE_SEG_BUCK;
	node_buck = bkdrhash%MEMSTORE_NODE_BUCK;
	seg = &(store->seg_map[seg_buck]);
	node = &(seg->node_array[seg->current_ptr]);
	bloom_add(seg->bloom, disk_hash_node->fingerprint);
	node->bkdrhash = bkdrhash;
	node->seg_offset = seg->current_ptr;
	node->dirty = 0;
	node->seg_hash_id = seg->seg_hash_id;
	node->bf_id = seg->bf_id;
	node->hit_count = 0;
	/*insert to the small hash map head*/
	node->next = seg->node_map[node_buck];
	seg->node_map[node_buck] = node;
	node->lru_next = NULL;
	node->lru_pre = NULL;
	memcpy(&(node->hash_node), disk_hash_node, sizeof(struct disk_hash_node));

	//memstore_log("%s, bkdrhash:%ld, seg buk:%d, node buck:%d, bfid: %ld\n",disk_hash_node->fingerprint, bkdrhash, seg_buck, node_buck, seg->bf_id);

	seg->current_ptr++;
	if(seg->current_ptr>=MEMSTORE_NODE_NUM)
	{
		/*this mem seg is full, create the new_seg, copy the seg to 
		 * th new_seg, then move to the l2 level, and write to disk
		 * for the current seg, reflash it as new*/

		mem_seg_write(seg); //write the current one to disk, currentlly, if we do simulate, no need
		new_seg = mem_seg_create();
		new_seg->seg_hash_id = seg->seg_hash_id;
		new_seg->bf_id = seg->bf_id;
		new_seg->dirty = seg->dirty;
		new_seg->current_ptr = seg->current_ptr;
		new_seg->cache_level = 2;
		new_seg->bloom = seg->bloom;
		new_seg->node_map = seg->node_map;
		new_seg->node_array = seg->node_array;

		/*follow the seg, be the first in the l2 cache of this buck*/
		new_seg->hash_next = seg->hash_next;
		new_seg->hash_pre = seg->hash_next->hash_pre;
		seg->hash_next->hash_pre = new_seg;
		seg->hash_next = new_seg;

		/*add to the l2 cache*/
		new_seg->lru_next = store->seg_lru.lru_next;
		new_seg->lru_pre = store->seg_lru.lru_next->lru_pre;
		store->seg_lru.lru_next->lru_pre = new_seg;
		store->seg_lru.lru_next = new_seg;

		store->seg_cache_num++;
		store->seg_total_num++;
		mem_seg_new(seg, store);
		
	}

	/*seg cache number is larger than pre-defined evict*/
	if(store->seg_cache_num>MEMSTORE_SEG_NUM)
	{
		memstore_log("write to the disk:%ld\n",store->seg_lru.lru_pre->bf_id);
		mem_node_cache_add(store, store->seg_lru.lru_pre); //need function
#ifdef MEMSTORE_SIMULATE
		store->seg_lru.lru_pre->cache_level = 3;
#else
		store->seg_lru.lru_pre->cache_level = 3;
		free(store->seg_lru.lru_pre->node_map);
		free(store->seg_lru.lru_pre->node_array);
		store->seg_lru.lru_pre->node_map=NULL;
		store->seg_lru.lru_pre->node_array=NULL;
#endif
		store->seg_cache_num--;

		/*if the bf cache is full, move to the null list l4 cache, just for testing*/
		mem_seg_lru_move_after(store->seg_lru.lru_pre, &(store->bf_lru));
		store->bf_cache_num++;
		if(store->bf_cache_num>MEMSTORE_BF_CACHE_NUM)
		{
#ifdef MEMSTORE_SIMULATE
			store->bf_lru.lru_pre->cache_level = 4;
#else
			store->bf_lru.lru_pre->cache_level = 4;
			bloom_destroy(store->bf_lru.lru_pre->bloom);
			store->bf_lru.lru_pre->bloom=NULL;
#endif
		}
		store->bf_cache_num--;
		mem_seg_lru_move_after(store->seg_lru.lru_pre, &(store->null_list));
	}
	
	return 0;
}


/*move the seg to LRU head*/
int mem_seg_lru_move_after(struct mem_seg *seg, struct mem_seg *lru)
{
	if(seg==NULL||lru==NULL)
		return -1;

	/*remove from original position*/
	seg->lru_next->lru_pre = seg->lru_pre;
	seg->lru_pre->lru_next = seg->lru_next;

	/*add after the lru location*/	
	seg->lru_next = lru->lru_next;
	seg->lru_pre = lru->lru_next->lru_pre;
	lru->lru_next->lru_pre = seg;
	lru->lru_next = seg;
	
	return 0;	

}



/*add the node to the cache
 * called when the mem_seg is removed from L2 cache and some node
 * will be added to the node cache */
int mem_node_cache_add(struct memstore *store, struct mem_seg *seg)
{
	if(store==NULL||seg==NULL)
		return -1;
	struct mem_node *node;
	int i, cache_buck;

	for(i=0;i<seg->current_ptr;i++)
	{
		if(seg->node_array[i].hit_count<(store->node_cache_hit_count/MEMSTORE_NODE_CACHE_NUM-1))
			continue;
		if(store->node_num==MEMSTORE_NODE_CACHE_NUM)
			mem_node_cache_evict(store);

		node = (struct memstore *)malloc(sizeof(struct memstore));
		memcpy(node, &(seg->node_array[i]), sizeof(struct memstore));
		cache_buck = node->bkdrhash%MEMSTORE_NODE_CACHE_BUCK;
		node->next = store->node_cache[cache_buck].next;
		store->node_cache[cache_buck].next = node;

		node->lru_next = store->node_cache[cache_buck].lru_next;
		node->lru_pre = store->node_cache[cache_buck].lru_next->lru_pre;
		store->node_cache[cache_buck].lru_next->lru_pre = node;
		store->node_cache[cache_buck].lru_next = node;

		store->node_cache_hit_count+=node->hit_count;
		store->node_num++;
	}
	
	
	return 0;
}


int mem_node_cache_evict(struct memstore *store)
{
	if(store==NULL)
		return -1;
	
	return 0;
}


/*checl if the chunk is in the l0 cache*/
struct mem_node *mem_node_cache_check(struct memstore *store, uint32_t bkdrhash, char *fingerprint)
{
	if(store==NULL)
		return NULL;
	int cache_buck;
	struct mem_node *node;

	cache_buck = bkdrhash%MEMSTORE_NODE_CACHE_BUCK;
	node = store->node_cache[cache_buck].next;
	while(node!=NULL)
	{
		if(memcmp(node->hash_node.fingerprint, fingerprint, FINGERPRINT_LEN+1)==0)
			return node;
		node = node->next;
	}
	return NULL;
}


/*if the seg is moved from l2 to l3 cache
 * it its content will be freed, so read it out from storage*/
int memstore_read_seg(struct mem_seg *seg)
{
	memstore_log("memstore_read_seg......:%ld\n", seg->bf_id);
	FILE *fp;
	char seg_path[PATH_MAX];
	char name[100];
	uint32_t bkdrhash;
	int node_buck, i;
	struct memstore *store;
	store = GET_FPDD_DATA()->dedup->memstore;
	sprintf(name, "/%ld", seg->bf_id);
	strcpy(seg_path,GET_FPDD_DATA()->memstoredir);
	strncat(seg_path, name, PATH_MAX);

	fp = fopen(seg_path, "r");
	if(fp==NULL)
	{
		error_log("memstore segment read failed!\n");
		return -1;
	}
	
	fread(&(seg->current_ptr), 1, sizeof(uint32_t), fp);
	if(seg->bloom==NULL)
	{
		seg->bloom = (struct _bloom *)malloc(sizeof(struct _bloom));
		seg->bloom->a = malloc(sizeof(char)*(BF_LEN+CHAR_BIT-1)/CHAR_BIT);
		fread(seg->bloom->a,(BF_LEN+CHAR_BIT-1)/CHAR_BIT, sizeof(char),fp);
		bloom_reload(seg->bloom);
	}
	fseek(fp,sizeof(uint32_t)+sizeof(char)*(BF_LEN+CHAR_BIT-1)/CHAR_BIT, SEEK_SET);
	seg->node_array = (struct mem_node *)malloc(seg->current_ptr*sizeof(struct mem_node));
	fread(seg->node_array, seg->current_ptr, sizeof(struct mem_node), fp);
	seg->node_map = (struct mem_node**)malloc(MEMSTORE_NODE_BUCK*sizeof(struct mem_node *));
	seg->cache_level = 2;
	fclose(fp);
	for(i=0;i<MEMSTORE_NODE_BUCK;i++)
	{
		seg->node_map[i]=NULL;
	}
	
	for(i=0;i<seg->current_ptr;i++)
	{
		bkdrhash = bkdr_hash(seg->node_array[i].hash_node.fingerprint);
		node_buck = bkdrhash%MEMSTORE_NODE_BUCK;
		seg->node_array[i].next = seg->node_map[node_buck];
		seg->node_map[node_buck] = &(seg->node_array[i]);
		seg->node_array[i].lru_next = NULL;
		seg->node_array[i].lru_pre = NULL;
	}
	store->seg_cache_num++;
	mem_seg_lru_move_after(seg, &(store->seg_lru));


	if(store->seg_cache_num>(MEMSTORE_SEG_NUM+8))
	{
		memstore_log("write to the disk:%ld\n",store->seg_lru.lru_pre->bf_id);
		mem_node_cache_add(store, store->seg_lru.lru_pre); //need function
		free(store->seg_lru.lru_pre->node_map);
		free(store->seg_lru.lru_pre->node_array);
		store->seg_lru.lru_pre->node_map=NULL;
		store->seg_lru.lru_pre->node_array=NULL;
		store->seg_cache_num--;

		/*if the bf cache is full, move to the null list*/
		mem_seg_lru_move_after(store->seg_lru.lru_pre, &(store->bf_lru));
		store->bf_cache_num++;
		if(store->bf_cache_num>MEMSTORE_BF_CACHE_NUM)
		{
			bloom_destroy(store->bf_lru.lru_pre->bloom);
			store->bf_lru.lru_pre->bloom=NULL;
		}
		store->bf_cache_num--;
		mem_seg_lru_move_after(store->seg_lru.lru_pre, &(store->null_list));
	}
	return 0;
}


/*when the seg is full, before it is moved to l2 cache
 * we first write it to storage to ensure its persistence*/
int mem_seg_write(struct mem_seg *seg)
{
	memstore_log("memstore_write_seg......:%ld\n", seg->bf_id);
	FILE *fp;
	char seg_path[PATH_MAX];
	char name[100];
	sprintf(name, "/%ld", seg->bf_id);
	strcpy(seg_path,GET_FPDD_DATA()->memstoredir);
	strncat(seg_path, name, PATH_MAX);
	fp = fopen(seg_path, "w+");
	if(fp==NULL)
	{
		error_log("memstore segment write failed!\n");
		return -1;
	}
	
	fwrite(&(seg->current_ptr), 1, sizeof(uint32_t),fp);
	fwrite(seg->bloom->a, ((BF_LEN+CHAR_BIT-1)/CHAR_BIT), sizeof(char), fp);
	fwrite(seg->node_array, seg->current_ptr, sizeof(struct mem_node), fp);
	fclose(fp);

	return 0;
}


/*after calculate the hash, we know the chunk id should be
 * find in the following seg buck, so check this seg*/
struct mem_node *memstore_seg_check(struct mem_seg *seg, char *fingerprint, uint32_t node_buck)
{
	//memstore_log("memstore_seg_check, %s,%ld, bf_id:%ld\n",fingerprint, node_buck, seg->bf_id);
	if(seg==NULL)
		return NULL;

	int ret=0;
	struct mem_node *node;

	if(seg->bloom==NULL)
	{
		//memstore_log("the pointer of seg content is empty\n");
		memstore_read_seg(seg);  
	}
	
	ret = bloom_check(seg->bloom, fingerprint);

	if(ret==0)
	{
		return NULL;
	}
	else
	{
		/*if the cache in l3, read it out from storage and move to l2*/
		if(seg->cache_level==3)
			memstore_read_seg(seg);

		node = seg->node_map[node_buck];
		while(node!=NULL)
		{
			if(memcmp(node->hash_node.fingerprint, fingerprint, FINGERPRINT_LEN+1)==0)
			{
				node->hit_count++;
				return node;
			}
			node = node->next;
		}
	}
	GET_FPDD_DATA()->BF_false = GET_FPDD_DATA()->BF_false+1;
	return NULL;
}
	


/*check the chunk id in the whole store
 * if not find, return null*/
struct mem_node *memstore_check(struct memstore *store, char *fingerprint)
{
	//memstore_log("memstore_check......\n");
	if(store==NULL)
		return NULL;
	
	uint32_t bkdrhash;
	int seg_buck, node_buck;
	struct mem_seg *seg, *new_seg;
	struct mem_node *node;
	node = NULL;

	bkdrhash = bkdr_hash(fingerprint);
	seg_buck = bkdrhash%MEMSTORE_SEG_BUCK;
	node_buck = bkdrhash%MEMSTORE_NODE_BUCK;

	/*first, check the l0 cache, if no ,return NULL*/
	node = mem_node_cache_check(store, bkdrhash, fingerprint);   //to be implemented
	
	if(node!=NULL)
		return node;

	/*then check each seg, from l1 to l3*/

	/*check the l1, the active one*/
	seg = &(store->seg_map[seg_buck]);
	node = memstore_seg_check(seg, fingerprint, node_buck);
	//memstore_log("%s, bkdrhash:%ld, seg buk:%d, node buck:%d, bfid: %ld\n", fingerprint, bkdrhash, seg_buck, node_buck, seg->bf_id);
	if(node!=NULL)
		return node;
	else
	{
		/*check the node in the l2 cache in this buck*/
		seg = store->seg_map[seg_buck].hash_next;
		while(seg!=&(store->seg_map[seg_buck]))
		{
			node = memstore_seg_check(seg, fingerprint, node_buck);
			if(node!=NULL)
			{
				/*if hit, the seg move to mru end of seg_lru*/
				mem_seg_lru_move_after(seg, &(store->seg_lru));
				return node;
			}
			else
				seg = seg->hash_next;
		}
	}

	return NULL;
}

	
	





































