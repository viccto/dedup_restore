/*
 ============================================================================
 Name        : chunk_cache.c
 Author      : Zhichao Cao
 Date        : 02/17/2017
 Copyright   : Your copyright notice
 Description : chunk caching for restore
 ============================================================================
 */


#include <fcntl.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <time.h> 

#include "enums.h"
#include "config.h"
#include "storagemanager.h"
#include "file.h"
#include "diskhash.h"
#include "data.h"
#include "container.h"
#include "list.h"
#include "metadata.h"
#include "cache.h"
#ifdef COMPRESS
#include "lzjb.h"
#endif
#include "dedup.h"
#include "chunk.h"
#include "optsmr.h"
#include "chunk_cache.h"

bool s_ptr_not_at_tail(void)
{
	bool ret;
	ret = true;
	if(GET_FPDD_DATA()->chunk_cache->lru.lru_pre_ptr == &(GET_FPDD_DATA()->chunk_cache->s_ptr))
		ret = false;
	return ret;
}


struct _chunk_cache_table *chunk_cache_init(void)
{
	struct _chunk_cache_table *chunk_cache;
	chunk_cache = (struct _chunk_cache_table *)malloc(sizeof(struct _chunk_cache_table));
	chunk_cache->cached_num = 0;
	chunk_cache->e_num=0;
	chunk_cache->p_num=0;
	chunk_cache->CHUNK_CACHE_NUM = CHUNK_CACHE_BUCK;
	/*lru cycle list*/
	chunk_cache->lru.lru_pre_ptr = &(chunk_cache->s_ptr); /*the lru end*/
	chunk_cache->lru.lru_next_ptr = &(chunk_cache->s_ptr); /*the mru end*/
	chunk_cache->s_ptr.lru_pre_ptr = &(chunk_cache->lru);
	chunk_cache->s_ptr.lru_next_ptr = &(chunk_cache->lru);
	int i;
	for(i=0;i<CHUNK_CACHE_BUCK; i++){
		chunk_cache->table[i].chunk_name = 0;
		chunk_cache->table[i].chunk_state = -1;
		chunk_cache->table[i].chunk_ptr = NULL;
		chunk_cache->table[i].lru_pre_ptr = NULL;
		chunk_cache->table[i].lru_next_ptr = NULL;
		/*hash map uses the cycle list*/
		chunk_cache->table[i].hm_pre_ptr = &(chunk_cache->table[i]);
		chunk_cache->table[i].hm_next_ptr = &(chunk_cache->table[i]);
	}
	return chunk_cache;
}

void chunk_cache_destroy(struct _chunk_cache_table *chunk_cache)
{
	int i;
	struct _chunk_cache_node *cur, *next, *pre;
	cur = chunk_cache->lru.lru_next_ptr;
	
	
	for(i=0;i<chunk_cache->cached_num;i++){
		if(cur==&(chunk_cache->lru))
			break;
		if(cur==&(chunk_cache->s_ptr))
			continue;
		next = cur->lru_next_ptr;
		free(cur->chunk_ptr);
		free(cur);
		cur = next;
	}
	free(chunk_cache);
}

void chunk_cache_remove_node(struct _chunk_cache_node *con_node)
{

	if((con_node ==NULL)||(con_node==&(GET_FPDD_DATA()->chunk_cache->lru))||(con_node==&(GET_FPDD_DATA()->chunk_cache->s_ptr)))
		return;
	/*remove from lru*/
	con_node->lru_next_ptr->lru_pre_ptr = con_node->lru_pre_ptr;
	con_node->lru_pre_ptr->lru_next_ptr = con_node->lru_next_ptr;

	/*remove from hash map*/
	con_node->hm_next_ptr->hm_pre_ptr = con_node->hm_pre_ptr;
	con_node->hm_pre_ptr->hm_next_ptr = con_node->hm_next_ptr;

	GET_FPDD_DATA()->chunk_cache->cached_num--;

	return;
}

uint64_t chunk_cache_evict(void)
{
	uint64_t chunk_name;
	struct _chunk_cache_node *tmp;
	tmp = GET_FPDD_DATA()->chunk_cache->lru.lru_pre_ptr;
	if(tmp==&(GET_FPDD_DATA()->chunk_cache->lru))
		return 0;
	chunk_name = tmp->chunk_name;

	/*free the content*/
	chunk_cache_remove_node(tmp);
	free(tmp->chunk_ptr);
	free(tmp);
	return chunk_name;
}
		

/*only the chunk data part pointer is added*/
int add_2_chunk_cache(char *chunk_ptr, uint64_t chunk_name)
{
	int pos,i;
	struct _chunk_cache_node *tmp, *evict_node;
	tmp = (struct _chunk_cache_node *)malloc(sizeof(struct _chunk_cache_node));
	tmp->chunk_name = chunk_name;
	tmp->chunk_ptr = chunk_ptr;
	tmp->chunk_state = 1; //e-chunk
	
	GET_FPDD_DATA()->chunk_cache->cached_num++;
		
	/*check the cached num, if larger, do the evict first*/
	while(GET_FPDD_DATA()->chunk_cache->cached_num>GET_FPDD_DATA()->chunk_cache->CHUNK_CACHE_NUM)
	{
		if(GET_FPDD_DATA()->chunk_cache->lru.lru_pre_ptr==&(GET_FPDD_DATA()->chunk_cache->s_ptr))
		{
			evict_node = GET_FPDD_DATA()->chunk_cache->s_ptr.lru_pre_ptr;
			GET_FPDD_DATA()->chunk_cache->p_num = 0;
			if(GET_FPDD_DATA()->chunk_cache->e_num>0)
				GET_FPDD_DATA()->chunk_cache->e_num--;
			else
				GET_FPDD_DATA()->chunk_cache->e_num = 0;
		}
		else
		{
			evict_node = GET_FPDD_DATA()->chunk_cache->lru.lru_pre_ptr;
			if(GET_FPDD_DATA()->chunk_cache->p_num>0)
				GET_FPDD_DATA()->chunk_cache->p_num--;
			else
				GET_FPDD_DATA()->chunk_cache->p_num = 0;
		}
		chunk_cache_remove_node(evict_node);
		free(evict_node->chunk_ptr);
		free(evict_node);
	}

	/*add to the LRU chain MRU end*/
	tmp->lru_next_ptr = GET_FPDD_DATA()->chunk_cache->lru.lru_next_ptr;
	tmp->lru_pre_ptr = GET_FPDD_DATA()->chunk_cache->lru.lru_next_ptr->lru_pre_ptr;
	GET_FPDD_DATA()->chunk_cache->lru.lru_next_ptr->lru_pre_ptr = tmp;
	GET_FPDD_DATA()->chunk_cache->lru.lru_next_ptr = tmp;
	
	/*add to the hashmap*/
	pos = chunk_name%CHUNK_CACHE_BUCK;
	tmp->hm_next_ptr = GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr;
	tmp->hm_pre_ptr = GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr->hm_pre_ptr;
	GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr->hm_pre_ptr = tmp;
	GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr = tmp;
	
	return 0;
}



	
struct _chunk_cache_node * find_chunk_cache(uint64_t chunk_name)
{
	struct _chunk_cache_node *tmp, *return_tmp;
	int pos;
	return_tmp=NULL;
	pos = chunk_name%CHUNK_CACHE_BUCK;
	tmp = &(GET_FPDD_DATA()->chunk_cache->table[pos]);
	while(tmp->hm_next_ptr!=&(GET_FPDD_DATA()->chunk_cache->table[pos]))
	{
		tmp = tmp->hm_next_ptr;
		if(tmp->chunk_name==chunk_name)
		{
			return_tmp = tmp;
			break;
		}
	}
	return return_tmp;
}



void chunk_cache_move_mru(struct _chunk_cache_node *tmp)
{
	int pos;
	chunk_cache_remove_node(tmp);
	
	GET_FPDD_DATA()->chunk_cache->cached_num++;
	/*add to the LRU chain MRU end*/
	tmp->lru_next_ptr = GET_FPDD_DATA()->chunk_cache->lru.lru_next_ptr;
	tmp->lru_pre_ptr = GET_FPDD_DATA()->chunk_cache->lru.lru_next_ptr->lru_pre_ptr;
	GET_FPDD_DATA()->chunk_cache->lru.lru_next_ptr->lru_pre_ptr = tmp;
	GET_FPDD_DATA()->chunk_cache->lru.lru_next_ptr = tmp;
	
	/*add to the hashmap*/
	pos = tmp->chunk_name%CHUNK_CACHE_BUCK;
	tmp->hm_next_ptr = GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr;
	tmp->hm_pre_ptr = GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr->hm_pre_ptr;
	GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr->hm_pre_ptr = tmp;
	GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr = tmp;

	if(tmp->chunk_state==0) /*p chunk*/
	{
		tmp->chunk_state = 1;
		GET_FPDD_DATA()->chunk_cache->e_num++;
		if(GET_FPDD_DATA()->chunk_cache->p_num>0)
			GET_FPDD_DATA()->chunk_cache->p_num--;
		else
			GET_FPDD_DATA()->chunk_cache->p_num = 0;
	}
	
	return;

}


void add_2_chunk_cache_head(struct _chunk_cache_node *tmp)
{
	int pos;
	struct _chunk_cache_node *evict_node;
	if(tmp==NULL)
		return;
	
	GET_FPDD_DATA()->chunk_cache->cached_num++;
	tmp->chunk_state = 1; // e-chunk
		
	/*check the cached num, if larger, do the evict first*/
	while(GET_FPDD_DATA()->chunk_cache->cached_num>GET_FPDD_DATA()->chunk_cache->CHUNK_CACHE_NUM)
	{
		if(GET_FPDD_DATA()->chunk_cache->lru.lru_pre_ptr==&(GET_FPDD_DATA()->chunk_cache->s_ptr))
		{
			evict_node = GET_FPDD_DATA()->chunk_cache->s_ptr.lru_pre_ptr;
			GET_FPDD_DATA()->chunk_cache->p_num = 0;
			if(GET_FPDD_DATA()->chunk_cache->e_num>0)
				GET_FPDD_DATA()->chunk_cache->e_num--;
			else
				GET_FPDD_DATA()->chunk_cache->e_num = 0;
		}
		else
		{
			evict_node = GET_FPDD_DATA()->chunk_cache->lru.lru_pre_ptr;
			if(GET_FPDD_DATA()->chunk_cache->p_num>0)
				GET_FPDD_DATA()->chunk_cache->p_num--;
			else
				GET_FPDD_DATA()->chunk_cache->p_num = 0;
		}
		chunk_cache_remove_node(evict_node);
		free(evict_node->chunk_ptr);
		free(evict_node);
	}
				

	/*add to the LRU chain after lru head*/
	tmp->lru_next_ptr = GET_FPDD_DATA()->chunk_cache->lru.lru_next_ptr;
	tmp->lru_pre_ptr = GET_FPDD_DATA()->chunk_cache->lru.lru_next_ptr->lru_pre_ptr;
	GET_FPDD_DATA()->chunk_cache->lru.lru_next_ptr->lru_pre_ptr = tmp;
	GET_FPDD_DATA()->chunk_cache->lru.lru_next_ptr = tmp;
	
	/*add to the hashmap*/
	pos = tmp->chunk_name%CHUNK_CACHE_BUCK;
	tmp->hm_next_ptr = GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr;
	tmp->hm_pre_ptr = GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr->hm_pre_ptr;
	GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr->hm_pre_ptr = tmp;
	GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr = tmp;
	
	return 0;
}


void add_2_chunk_cache_after_s_prt(struct _chunk_cache_node *tmp)
{
	int pos;
	struct _chunk_cache_node *evict_node;
	if(tmp==NULL)
		return;
	
	GET_FPDD_DATA()->chunk_cache->cached_num++;
	tmp->chunk_state = 0; // p-chunk
		
	/*check the cached num, if larger, do the evict first*/
	while(GET_FPDD_DATA()->chunk_cache->cached_num>GET_FPDD_DATA()->chunk_cache->CHUNK_CACHE_NUM)
	{
		if(GET_FPDD_DATA()->chunk_cache->lru.lru_pre_ptr==&(GET_FPDD_DATA()->chunk_cache->s_ptr))
		{
			evict_node = GET_FPDD_DATA()->chunk_cache->s_ptr.lru_pre_ptr;
			GET_FPDD_DATA()->chunk_cache->p_num = 0;
			if(GET_FPDD_DATA()->chunk_cache->e_num>0)
				GET_FPDD_DATA()->chunk_cache->e_num--;
			else
				GET_FPDD_DATA()->chunk_cache->e_num = 0;
		}
		else
		{
			evict_node = GET_FPDD_DATA()->chunk_cache->lru.lru_pre_ptr;
			if(GET_FPDD_DATA()->chunk_cache->p_num>0)
				GET_FPDD_DATA()->chunk_cache->p_num--;
			else
				GET_FPDD_DATA()->chunk_cache->p_num = 0;
		}
		chunk_cache_remove_node(evict_node);
		free(evict_node->chunk_ptr);
		free(evict_node);
	}
				

	/*add to the LRU chain after s_ptr*/
	tmp->lru_next_ptr = GET_FPDD_DATA()->chunk_cache->s_ptr.lru_next_ptr;
	tmp->lru_pre_ptr = GET_FPDD_DATA()->chunk_cache->s_ptr.lru_next_ptr->lru_pre_ptr;
	GET_FPDD_DATA()->chunk_cache->s_ptr.lru_next_ptr->lru_pre_ptr = tmp;
	GET_FPDD_DATA()->chunk_cache->s_ptr.lru_next_ptr = tmp;
	
	/*add to the hashmap*/
	pos = tmp->chunk_name%CHUNK_CACHE_BUCK;
	tmp->hm_next_ptr = GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr;
	tmp->hm_pre_ptr = GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr->hm_pre_ptr;
	GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr->hm_pre_ptr = tmp;
	GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr = tmp;
	
	GET_FPDD_DATA()->chunk_cache->p_num++;

	return;
}

void add_2_chunk_cache_before_s_prt(struct _chunk_cache_node *tmp)
{
	int pos;
	struct _chunk_cache_node *evict_node;
	if(tmp==NULL)
		return;
	
	GET_FPDD_DATA()->chunk_cache->cached_num++;
	tmp->chunk_state = 1; // e-chunk
		
	/*check the cached num, if larger, do the evict first*/
	while(GET_FPDD_DATA()->chunk_cache->cached_num>GET_FPDD_DATA()->chunk_cache->CHUNK_CACHE_NUM)
	{
		if(GET_FPDD_DATA()->chunk_cache->lru.lru_pre_ptr==&(GET_FPDD_DATA()->chunk_cache->s_ptr))
		{
			evict_node = GET_FPDD_DATA()->chunk_cache->s_ptr.lru_pre_ptr;
			GET_FPDD_DATA()->chunk_cache->p_num = 0;
			if(GET_FPDD_DATA()->chunk_cache->e_num>0)
				GET_FPDD_DATA()->chunk_cache->e_num--;
			else
				GET_FPDD_DATA()->chunk_cache->e_num = 0;
		}
		else
		{
			evict_node = GET_FPDD_DATA()->chunk_cache->lru.lru_pre_ptr;
			if(GET_FPDD_DATA()->chunk_cache->p_num>0)
				GET_FPDD_DATA()->chunk_cache->p_num--;
			else
				GET_FPDD_DATA()->chunk_cache->p_num = 0;
		}
		chunk_cache_remove_node(evict_node);
		free(evict_node->chunk_ptr);
		free(evict_node);
	}
				

	/*add to the LRU chain before s_ptr*/
	tmp->lru_next_ptr = GET_FPDD_DATA()->chunk_cache->s_ptr.lru_pre_ptr->lru_next_ptr;
	tmp->lru_pre_ptr = GET_FPDD_DATA()->chunk_cache->s_ptr.lru_pre_ptr;
	GET_FPDD_DATA()->chunk_cache->s_ptr.lru_pre_ptr->lru_next_ptr = tmp;
	GET_FPDD_DATA()->chunk_cache->s_ptr.lru_pre_ptr = tmp;
	
	/*add to the hashmap*/
	pos = tmp->chunk_name%CHUNK_CACHE_BUCK;
	tmp->hm_next_ptr = GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr;
	tmp->hm_pre_ptr = GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr->hm_pre_ptr;
	GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr->hm_pre_ptr = tmp;
	GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr = tmp;

	GET_FPDD_DATA()->chunk_cache->e_num++;

	return;
}

void move_2_chunk_cache_head(struct _chunk_cache_node *tmp)
{
	int pos;
	if(tmp==NULL)
		return;
	chunk_cache_remove_node(tmp);
	
	GET_FPDD_DATA()->chunk_cache->cached_num++;
		/*add to the LRU chain after lru head*/
	tmp->lru_next_ptr = GET_FPDD_DATA()->chunk_cache->lru.lru_next_ptr;
	tmp->lru_pre_ptr = GET_FPDD_DATA()->chunk_cache->lru.lru_next_ptr->lru_pre_ptr;
	GET_FPDD_DATA()->chunk_cache->lru.lru_next_ptr->lru_pre_ptr = tmp;
	GET_FPDD_DATA()->chunk_cache->lru.lru_next_ptr = tmp;
	
	/*add to the hashmap*/
	pos = tmp->chunk_name%CHUNK_CACHE_BUCK;
	tmp->hm_next_ptr = GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr;
	tmp->hm_pre_ptr = GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr->hm_pre_ptr;
	GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr->hm_pre_ptr = tmp;
	GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr = tmp;

	/*check and mark the chunk state as e or p*/
	if(tmp->chunk_state==0) /*p chunk*/
	{
		tmp->chunk_state = 1;
		GET_FPDD_DATA()->chunk_cache->e_num++;
		if(GET_FPDD_DATA()->chunk_cache->p_num>0)
			GET_FPDD_DATA()->chunk_cache->p_num--;
		else
			GET_FPDD_DATA()->chunk_cache->p_num = 0;
	}

	return 0;
}


void move_2_chunk_cache_after_s_prt(struct _chunk_cache_node *tmp)
{
	int pos;
	if(tmp==NULL)
		return;
	chunk_cache_remove_node(tmp);

	GET_FPDD_DATA()->chunk_cache->cached_num++;
	/*add to the LRU chain before s_ptr*/
	tmp->lru_next_ptr = GET_FPDD_DATA()->chunk_cache->s_ptr.lru_next_ptr;
	tmp->lru_pre_ptr = GET_FPDD_DATA()->chunk_cache->s_ptr.lru_next_ptr->lru_pre_ptr;
	GET_FPDD_DATA()->chunk_cache->s_ptr.lru_next_ptr->lru_pre_ptr = tmp;
	GET_FPDD_DATA()->chunk_cache->s_ptr.lru_next_ptr = tmp;
	
	/*add to the hashmap*/
	pos = tmp->chunk_name%CHUNK_CACHE_BUCK;
	tmp->hm_next_ptr = GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr;
	tmp->hm_pre_ptr = GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr->hm_pre_ptr;
	GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr->hm_pre_ptr = tmp;
	GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr = tmp;

	/*check and mark the chunk state as e or p*/
	if(tmp->chunk_state==1) /*e chunk*/
	{
		tmp->chunk_state = 0;
		if(GET_FPDD_DATA()->chunk_cache->e_num>0)
			GET_FPDD_DATA()->chunk_cache->e_num--;
		else
			GET_FPDD_DATA()->chunk_cache->e_num = 0;
		GET_FPDD_DATA()->chunk_cache->p_num++;
	}

	return;
}


void move_2_chunk_cache_before_s_prt(struct _chunk_cache_node *tmp)
{
	int pos;
	if(tmp==NULL)
		return;
	chunk_cache_remove_node(tmp);

	GET_FPDD_DATA()->chunk_cache->cached_num++;
	/*add to the LRU chain after s_ptr*/
	tmp->lru_next_ptr = GET_FPDD_DATA()->chunk_cache->s_ptr.lru_pre_ptr->lru_next_ptr;
	tmp->lru_pre_ptr = GET_FPDD_DATA()->chunk_cache->s_ptr.lru_pre_ptr;
	GET_FPDD_DATA()->chunk_cache->s_ptr.lru_pre_ptr->lru_next_ptr = tmp;
	GET_FPDD_DATA()->chunk_cache->s_ptr.lru_pre_ptr = tmp;
	
	/*add to the hashmap*/
	pos = tmp->chunk_name%CHUNK_CACHE_BUCK;
	tmp->hm_next_ptr = GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr;
	tmp->hm_pre_ptr = GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr->hm_pre_ptr;
	GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr->hm_pre_ptr = tmp;
	GET_FPDD_DATA()->chunk_cache->table[pos].hm_next_ptr = tmp;

	/*check and mark the chunk state as e or p*/
	if(tmp->chunk_state==0) /*e chunk*/
	{
		tmp->chunk_state = 1;
		GET_FPDD_DATA()->chunk_cache->e_num++;
		if(GET_FPDD_DATA()->chunk_cache->p_num>0)
			GET_FPDD_DATA()->chunk_cache->p_num--;
		else
			GET_FPDD_DATA()->chunk_cache->p_num = 0;
	}

	return;
}

/*get the size of p-chache size, in chunk num granularity*/
int chunk_cache_pcache_size(void)
{
	/*
	if(GET_FPDD_DATA()->chunk_cache->cached_num<GET_FPDD_DATA()->chunk_cache->CHUNK_CACHE_NUM)
		return (GET_FPDD_DATA()->chunk_cache->CHUNK_CACHE_NUM-GET_FPDD_DATA()->chunk_cache->e_num);
	else
	*/
		return GET_FPDD_DATA()->chunk_cache->p_num;
}





		
