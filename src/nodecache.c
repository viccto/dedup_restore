/*
 ============================================================================
 Name        : nodecache.c
 Author      : Zhichao Cao
 Date        : 11/30/2016
 Copyright   : Your copyright notice
 Description : the cache function for the deduplication metadata, read and write cache
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
#include "nodecache.h"

#define offsetof(TYPE, MEMBER) ((int)(&((TYPE *)0)->MEMBER))


struct node_cache_header *write_cache_init()
{
    container_log("into the cache init\n");
	struct node_cache_header *cache_header;
	cache_header = (struct node_cache_header *)malloc(sizeof(struct node_cache_header));
	pthread_spin_init(&(cache_header->cache_lock),0);
	cache_header->next = NULL;
	cache_header->size = 0;
    container_log("into the cache head pointer:%p\n",cache_header);
	return cache_header;
}


/*add the dedup_node after the head*/
int write_cache_head_add(struct node_cache_header *cache_head, struct disk_hash_node *node)
{
    container_log("add the node to write cache: %lld, cache head pointer:%p, the cache_head->next: %p\n",node->counter,cache_head, cache_head->next);
	struct node_cache *new_cache_node;
    new_cache_node = NULL;
	new_cache_node = (struct node_cache *)malloc(sizeof(struct node_cache));
	memcpy(&(new_cache_node->node), node, sizeof(struct disk_hash_node));
	pthread_spin_lock(&(cache_head->cache_lock));
	new_cache_node->next = cache_head->next;
	cache_head->next = new_cache_node;
	cache_head->size++;
	pthread_spin_unlock(&(cache_head->cache_lock));

    container_log("the write cache size: %d\n", cache_head->size);
	return 0;
}

/*remove the node from head, and put the pointer to the disk hash node, free the cache node*/
int write_cache_head_remove(struct node_cache_header *cache_head, struct disk_hash_node *node)
{
	pthread_spin_lock(&(cache_head->cache_lock));
	struct node_cache *tmp;
	tmp = cache_head->next;
	if(tmp!=NULL){
		memcpy(node, &(tmp->node), sizeof(struct disk_hash_node));
		cache_head->next = tmp->next;
		free(tmp);
		cache_head->size--;
		pthread_spin_unlock(&(cache_head->cache_lock));
		return 0;
	}
	else{
		pthread_spin_unlock(&(cache_head->cache_lock));
		return -1;
	}
}


int write_cache_head_remove_unser(struct node_cache_header *cache_head, struct disk_hash_node *node)
{
	struct node_cache *tmp;
	tmp = cache_head->next;
	if(tmp!=NULL){
		memcpy(node, &(tmp->node), sizeof(struct disk_hash_node));
		cache_head->next = tmp->next;
		free(tmp);
		cache_head->size--;
		return 1;
	}
	else{
		return 0;
	}
}


/*lookup the fingerprint in the write cache,*/
struct disk_hash_node * write_cache_lookup(struct node_cache_header *cache_head, char fingerprint[FINGERPRINT_LEN])
{
	struct node_cache * tmp;
    struct disk_hash_node * return_node;
	tmp = cache_head->next;

	//pthread_spin_lock(&(cache_head->cache_lock));
	while(tmp!=NULL){
        //container_log("original fingerprint: %s, and the tmp->fingerprint: %s, counter is: %d\n", fingerprint, tmp->node.fingerprint, tmp->node.counter);
		if(0 == memcmp(tmp->node.fingerprint, fingerprint, FINGERPRINT_LEN)){
			//pthread_spin_unlock(&(cache_head->cache_lock));
			return_node = &(tmp->node);
            container_log("find in the write cache, chunk_id: %lld, container_id: %lld, t2_count %lld\n", return_node->counter, return_node->container_name, return_node->t2_count);
            return return_node;
		}
		tmp = tmp->next;
	}
	//pthread_spin_unlock(&(cache_head->cache_lock));

	return NULL;
}


/*destroy the write cache and clean the head*/
int write_cache_destroy(struct node_cache_header *cache_head)
{
    container_log("destroy the write cache\n");

	struct node_cache * tmp;

	pthread_spin_lock(&(cache_head->cache_lock));
	tmp = cache_head->next;
	while(tmp!=NULL){
		cache_head->next = tmp->next;
		free(tmp);
		cache_head->size--;
		tmp = cache_head->next;
	}
	pthread_spin_unlock(&(cache_head->cache_lock));

	free(cache_head);
	return 0;
}
		
			







