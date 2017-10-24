/*
 ============================================================================
 Name        : container.c
 Author      : Zhichao Cao
 Date        : 05/10/2016
 Copyright   : Your copyright notice
 Description : container related functions
 ============================================================================
 */


#include <fcntl.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <time.h>
#include <unistd.h> 

#include "enums.h"
#include "config.h"
#include "storagemanager.h"
#include "file.h"
#include "disk.h"
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
#include "smr.h"

/*write containter 0 with full content 'w'*/
void write_standard_container(void)
{
	struct container *container;
	container = container_init(0);
	memset(container->data, 'w', MAX_CONTAINER_DATA);
	write_container(container);
	return;
}
	

struct container *container_init(uint64_t container_name)
{
	dedup_log("get int the container_init...............\n");
	struct container *container;
	container = malloc(sizeof(struct container));
	if(container==NULL)
	{
		error_log("container initiate falled!\n");
		return NULL;
	}
	container->header.chunk_num = 0;
	container->header.container_name = container_name;
	container->header.data_size = 0;
	container->header.data_offset = 0;
	container->header.stream_id = 0;
	return container;
}

void container_destroy(struct container *container)
{
	write_container(container);
	free(container);
	return;
}

struct _zone_container_table *zone_container_table_init()
{
	container_log("enter zone_container_table inti...\n");
	uint32_t zone_num,  zone_size;
	int i, ret=0;
	ret = smr_states(&zone_num, &zone_size);
	struct _zone_container_table *zone_container_table;
	if(ret)
	{
		error_log("get smr information failed!\n");
		return NULL;
	}
	zone_container_table = (struct _zone_container_table *)malloc(sizeof(struct _zone_container_table));
	zone_container_table->zone_num = zone_num;
	if(zone_container_table==NULL)
	{
		error_log("init zone_container_table failed!\n");
		return NULL;
	}
	container_log("start to init the zone total chunk and free chunk\n");
	zone_container_table->zone_total_chunk = (uint32_t *)malloc(zone_num*sizeof(uint32_t));
	zone_container_table->zone_free_chunk = (uint32_t *)malloc(zone_num*sizeof(uint32_t));
	zone_container_table->current_pointer = (uint32_t *)malloc(zone_num*sizeof(uint32_t));
	for(i=0;i<zone_num;i++)
	{
		zone_container_table->zone_total_chunk[i] = 0; /*zone utilization*/
		zone_container_table->zone_free_chunk[i] = 0; /*zone utilization*/
		zone_container_table->current_pointer[i] = 0;
	}
	
	zone_container_table->container_table = container_table_init(zone_num, zone_size);
	container_log("exit contaienr_table_int,,,,,,\n");
	return zone_container_table;
}


void zone_container_table_destroy(struct _zone_container_table *zone_container_table)
{
	/* TODO fix the incompatilbe parameter type for the 
	 * following function -Fenggang
	 */
	write_zone_container_table(zone_container_table);
	free(zone_container_table->zone_total_chunk);
	free(zone_container_table->zone_free_chunk);
	container_table_destroy(zone_container_table->container_table, zone_container_table->zone_num);
	return;
}
		

/*initate the contaienr table for future use*/
struct container_info **container_table_init(uint32_t zone_num, uint32_t zone_size)
{
	container_log("enter contaienr_table_init..... \n");
	uint32_t container_num_per_zone;
	int i;
	struct container_info **zone_container_table;
	container_num_per_zone = zone_size/MAX_CONTAINER_SIZE;
	
	zone_container_table = (struct container_info **)malloc(zone_num*sizeof(struct container_info *));
	if(zone_container_table==NULL)
	{
		error_log("init zone_container_table failed!\n");
		return NULL;
	}
	for(i=0;i<zone_num;i++)
	{
		zone_container_table[i] = (struct container_info*)malloc(container_num_per_zone*sizeof(struct container_info));
		if(zone_container_table[i]==NULL)
		{
			error_log("init zone_container_table failed!\n");
			return NULL;
		}
	}
	container_log("exit container_table_init....\n");
	return zone_container_table;
}

/*free the container table and write to T1*/
void container_table_destroy(struct container_info **zone_table, uint32_t zone_num)
{
	int i;
	for(i=0;i<zone_num;i++)
	{
		free(zone_table[i]);
	}
	free(zone_table);
}

/*after GC, the zone table information at this zone should be cleaned*/
void reset_zone_table_at_zone(int zone)
{
	int i;
	struct _zone_container_table *zone_table;
	zone_table = GET_FPDD_DATA()->zone_container_table;
	
	for(i=0; i<zone_table->current_pointer[zone];i++){
		zone_table->container_table[zone][i].container_name = 0;
		zone_table->container_table[zone][i].zone = 0;
	}
	zone_table->zone_total_chunk[zone] = 0; /*zone utilization*/
	zone_table->zone_free_chunk[zone] = 0; /*zone utilization*/
	zone_table->current_pointer[zone] = 0;
	return;
}

/*fake function just used to debug in local*/
int smr_get_zoneidx_fake(uint64_t container_name)
{
	int zone_size, containers_per_zone, zone;
	zone_size = ZONE_SIZE_FAKE;
	containers_per_zone = zone_size/MAX_CONTAINER_SIZE;
	zone = container_name/containers_per_zone;
	return zone;
}

/*get the zone number from the container name*/
int smr_get_zone_id(uint64_t container_name)
{
#ifdef SMR
			return smr_get_zoneidx(container_name);
#else
			return smr_get_zoneidx_fake(container_name);
#endif
}


/*when the chunk's counter <=0, increase the free*/
void increase_zone_free(uint64_t container_name)
{
	int32_t zone;
	zone = smr_get_zone_id(container_name);
	struct _zone_container_table * zone_table;
	zone_table = GET_FPDD_DATA()->zone_container_table;
	zone_table->zone_free_chunk[zone]++;
	return;

}

/*when the chunk reference is 0, but re-added before GC, 
 * decrease the free number*/
void decrease_zone_free(uint64_t container_name)
{
	int32_t zone;
	zone = smr_get_zone_id(container_name);
	struct _zone_container_table * zone_table;
	zone_table = GET_FPDD_DATA()->zone_container_table;
	zone_table->zone_free_chunk[zone]--;
	return;
}

/*print the zone_table content for debug*/
void print_zone_table()
{
	container_log("print the zone table\n");
	struct _zone_container_table * zone_table;
	zone_table = GET_FPDD_DATA()->zone_container_table;
	int i;
	for(i=0;i<10;i++){
		container_log("zone total chunk: %d\n", zone_table->zone_total_chunk[i]);
		container_log("zone free chunk: %d\n", zone_table->zone_free_chunk[i]);
	}
	return;
}
	

/*write the mapping table to T1 so that can be used in the future
 *but nut implemented yet	*/
void write_zone_container_table(struct  container_info **zone_table)
{
	return;
}

/*set the stream id in different mode*/
int set_stream_id(void)
{
	if(NEAR)
		return 0;
	if(RANDOM){
		srand((unsigned)time(NULL));
		return rand()%GET_FPDD_DATA()->stream_id_limit;
	}
	if(CYCLE){
		GET_FPDD_DATA()->stream_id++;
		if(GET_FPDD_DATA()->stream_id>=GET_FPDD_DATA()->stream_id_limit)
			GET_FPDD_DATA()->stream_id = 0;
		}
	return GET_FPDD_DATA()->stream_id;
}

int new_container(uint64_t container_name)
{
	struct container *container;
	container = GET_FPDD_DATA()->container;
	dedup_log("create a new container.............\n");
	container->header.chunk_num = 0;
	container->header.container_name = container_name;
	container->header.data_size = 0;
	container->header.data_offset = 0;
	container->header.stream_id = set_stream_id();
	return 0;
}

/*write the container to a new file*/
int write_container(struct container *container)
{
	dedup_log("write container to as a file.........\n");
	FILE *fp;
	char container_path[PATH_MAX];
	char name[100];
	
	sprintf(name, "/%ld", container->header.container_name);
	strcpy(container_path,GET_FPDD_DATA()->dedupedchunkdir);
	strncat(container_path, name, PATH_MAX);
	
	fp = fopen(container_path, "w+");
	if(fp==NULL)
	{
		error_log("container write failed!\n");
		return -1;
	}
	fwrite(container, 1, sizeof(struct container), fp);
	fclose(fp);
	

	/*update the zone_container_table information*/
	struct _zone_container_table * zone_table;
	int zone, pos_in_zone;
	zone_table = GET_FPDD_DATA()->zone_container_table;
	zone = smr_get_zone_id(container->header.container_name);
	/*
	pos_in_zone = container->header.container_name%(ZONE_SIZE_FAKE/MAX_CONTAINER_SIZE);
	zone_table->zone_total_chunk[zone]+= container->header.chunk_num;
	zone_table->container_table[zone][pos_in_zone].container_name = container->header.container_name;
	zone_table->container_table[zone][pos_in_zone].zone = zone;
	*/
	pos_in_zone = zone_table->current_pointer[zone];
	zone_table->current_pointer[zone]++;
	zone_table->zone_total_chunk[zone]+= container->header.chunk_num;
	zone_table->container_table[zone][pos_in_zone].container_name = container->header.container_name;
	zone_table->container_table[zone][pos_in_zone].zone = zone;

	dedup_log("write operation finished.........\n");
	return zone;	
}

int write_container_2_smr(struct container *container)
{
	size_t ret;
	int zone;
	struct _zone_container_table *zone_table;
	zone_table = GET_FPDD_DATA()->zone_container_table;
	ret = smr_write(container->header.container_name, (char *)container, MAX_CONTAINER_SIZE, container->header.stream_id, &zone);
	if((ret<0)&&(zone>=0))
	{
		error_log("write to smr failed!\n");
		return -1;
	}
	test_log("write_container_2_smr, the zone is: %d\n", zone);
	/*increase the total chunk num in the zone*/
	zone_table->zone_total_chunk[zone]+=container->header.chunk_num;
	/*remember the container name sequentially in the array*/
	zone_table->container_table[zone][zone_table->current_pointer[zone]].container_name = container->header.container_name;
	zone_table->container_table[zone][zone_table->current_pointer[zone]].zone = zone;
	/*increase the current pointer, point to the next position for write*/
	zone_table->current_pointer[zone]++;
	return zone;
	
}

int read_container(struct container *buf, uint64_t container_name)
{
#ifdef SMR
			return read_contaienr_from_smr(buf, container_name);
#else
			return _read_container(buf, container_name);
#endif
}


/*read container from file, for HDD use*/
int _read_container(struct container *buf, uint64_t container_name)
{
		char container_path[PATH_MAX];
		char name[100];
		char * data;
		FILE *fp;
	
		sprintf(name, "/%d", container_name);
		strcpy(container_path,GET_FPDD_DATA()->dedupedchunkdir);
		strncat(container_path, name, PATH_MAX);
		dedup_log("container path: %s\n",container_path);
		fp = fopen(container_path, "r");
		if(fp==NULL)
		{
			error_log("container read failed!\n");
			return -1;
		}


		fread(buf, 1, sizeof(struct container), fp);
		dedup_log("get the data of the whole container and retrun\n");
		fclose(fp);
		return 0;
}


int read_contaienr_from_smr(struct container *container, uint64_t container_name)
{
	size_t ret;
	ret = smr_read(container_name, (char *)container, MAX_CONTAINER_SIZE);
	if(ret<=0)
	{
		error_log("read container from smr failed\n");
		return -1;
	}
	return 0;
}

/*read out the data in container to buff, 
 * only data in the buff, head is not included 
 * must be used paired with release_container*/
int grabe_container(char *buff, struct container_header *header, uint64_t container_name)
{
	dedup_log("grabe_container and write data to buffer..........\n");
	FILE *fp;
	char container_path[PATH_MAX];
	char name[100];
	struct container *container_file;
	
	sprintf(name, "/%ld", container_name);
	strcpy(container_path,GET_FPDD_DATA()->dedupedchunkdir);
	strncat(container_path, name, PATH_MAX);
	dedup_log("container path: %s\n",container_path);

	fp = fopen(container_path, "r");
	if(fp==NULL)
	{
		error_log("container read failed!\n");
		return -1;
	}
	container_file = malloc(sizeof(struct container));
	fread(container_file, 1, sizeof(struct container), fp);
	memcpy(buff, &(container_file->data), MAX_CONTAINER_DATA);
	dedup_log("get the data of the whole container and retrun\n");
	free(container_file);
	fclose(fp);
	return 0;
}

void release_container(char *buff)
{
	free(buff);
	return;
}

/*current logic is, check the container itself in this function
 * make sure that the chunk number, size does not larger than 
 * the size of container limit. if larger, write the current one
 * create a new container, return the container name*/
uint64_t add_2_container(char *buff, uint32_t len, char fingerprint[FINGERPRINT_LEN], uint64_t counter, struct chunk_index *index)
{
	struct container * container;
	int zone_id;
	container = GET_FPDD_DATA()->container;
	dedup_log("add data to container.......\n");
	if((container->header.data_size+len)>MAX_CONTAINER_DATA)
	{
		dedup_log("need new container, chunk num: %d, data size: %d,container size:%d, container data size:%d\n",container->header.chunk_num,container->header.data_size, sizeof(struct container), MAX_CONTAINER_DATA);

		/*here add the function to decide the zone for container*/
		//
#ifdef CONTAINER_FREE_DEDUP
	zone_id = smr_get_zone_id(container->header.container_name);
#else
	#ifdef SMR
			zone_id = write_container_2_smr(container);
	#else
			zone_id = write_container(container);
	#endif
#endif
		flush_write_cache(zone_id);
		GET_FPDD_DATA()->container_counter++;
		dedup_log("new container name: %d\n", GET_FPDD_DATA()->container_counter);
		new_container(GET_FPDD_DATA()->container_counter);
	}
	container = GET_FPDD_DATA()->container;
	memcpy(index->fingerprint, fingerprint, FINGERPRINT_LEN);
	memcpy(container->header.index[container->header.chunk_num].fingerprint, fingerprint, FINGERPRINT_LEN);
	dedup_log("start to get container prameter....\n");
	index->offset = container->header.data_offset;
	container->header.index[container->header.chunk_num].offset = container->header.data_offset;
	index->size = len;
	container->header.index[container->header.chunk_num].size = len;
	index->counter = counter;
	container->header.index[container->header.chunk_num].counter = counter;
	//container_log("the new chunk->: data offset: %d, data len: %d, contianer name:%lld, chunk counter:%lld \n",index->offset,index->size,container->header.container_name, index->counter);
	char *data_pos;
	data_pos = &(container->data[container->header.data_offset]);
	
	
	memcpy(data_pos, buff, len);
	
	container->header.chunk_num++;
	container->header.data_size = container->header.data_size+len;
	container->header.data_offset = container->header.data_offset+len;
	
	return container->header.container_name;
}

/*read the whole container to the read buff for look ahead restore*/
int read_container_to_buf(char *buff, uint64_t container_name)
{
	struct container *container, *container_file;
	char *data;
	int ret;

	container = GET_FPDD_DATA()->container;
#ifdef CONTAINER_FREE_DEDUP
	container_file = malloc(sizeof(struct container));
	read_container(container_file, 0);
	data  = &(container_file->data);
	memcpy(buff, data, MAX_CONTAINER_DATA);
	free(container_file);
	usleep(CONTAINER_IO_DELAY);
#else

	if(container_name==container->header.container_name)
	{
		data = &(container->data);
		memcpy(buff, data, MAX_CONTAINER_DATA);
	}
	else
	{

		container_file = malloc(sizeof(struct container));
		read_container(container_file, container_name);
		data  = &(container_file->data);
		memcpy(buff, data, MAX_CONTAINER_DATA);
		free(container_file);
	}
#endif
	return 0;
}

uint64_t get_chunk(char *buff, uint32_t offset, uint32_t len, char fingerprint[FINGERPRINT_LEN], uint64_t container_name)
{
	dedup_log("get chunk from container................\n");
	//struct container_header *header;
	struct container *container, *container_file;
	char *data;
	int ret;
//	int i;
	 dedup_log("the original address: container_name%lld, offset %d\n",container_name, offset);
	container = GET_FPDD_DATA()->container;
	if(container_name==container->header.container_name)
	{
		dedup_log("the chunk is in memory container.......\n");
		data = &(container->data);
		memcpy(buff, data+offset, len);
	}
	else
	{
		//header = malloc(sizeof(struct container_header));
		//ret = grabe_container(data, header, container_name);
		container_file = malloc(sizeof(struct container));
		if(smr_get_zone_id(container_name)<0){
			dedup_log("container old addess invalide: container_name%lld\n",container_name);
			//ret = get_chunk_address(GET_FPDD_DATA()->dedup, fingerprint, &container_name, &offset);
			dedup_log("container new addess: container_name%lld, offset %d\n",container_name, offset);
			if(!ret){
				error_log("can not find the chunk!\n");
				return -1;
			}
		}
		if(container_name==container->header.container_name)
		{
			dedup_log("the chunk is in memory container.......\n");
			data = &(container->data);
			memcpy(buff, data+offset, len);
		}
		else{
			read_container(container_file, container_name);
			data  = &(container_file->data);
			memcpy(buff, data+offset, len);
			free(container_file);
		}
	}
	return 0;
}

uint64_t get_container_header(struct container_header *header, uint64_t container_name)
{
	FILE *fp;
	char container_path[PATH_MAX];
	char name[100];
	
	sprintf(name, "/%d", container_name);
	strcpy(container_path,GET_FPDD_DATA()->dedupedchunkdir);
	strncat(container_path, name, PATH_MAX);

	fp = fopen(container_path, "r");
	if(fp==NULL)
	{
		error_log("container read failed!\n");
		return -1;
	}
	fseek(fp, 0, SEEK_SET);
	fread(header, 1, sizeof(struct container_header), fp);
	fclose(fp);
	return 0;
}

int container_exist_fake(uint64_t container_name)
{
	FILE *fp;
	char container_path[PATH_MAX];
	char name[100];
	
	sprintf(name, "/%d", container_name);
	strcpy(container_path,GET_FPDD_DATA()->dedupedchunkdir);
	strncat(container_path, name, PATH_MAX);
	
	if((!access(container_path,0))||(container_name==GET_FPDD_DATA()->container->header.container_name))
		return 1; //exist
	else
		return 0; //not here

}

/*****************************container cache***********************/

struct _container_cache_table *container_cache_init(void)
{
	struct _container_cache_table *container_cache;
	container_cache = (struct _container_cache_table *)malloc(sizeof(struct _container_cache_table));
	container_cache->cached_num = 0;
	/*lru cycle list*/
	container_cache->lru.lru_pre_ptr = &(container_cache->lru); /*the lru end*/
	container_cache->lru.lru_next_ptr = &(container_cache->lru); /*the mru end*/
	int i;
	for(i=0;i<CONTAINER_CACHE_BUCK; i++){
		container_cache->table[i].container_name = 0;
		container_cache->table[i].container_ptr = NULL;
		container_cache->table[i].lru_pre_ptr = NULL;
		container_cache->table[i].lru_next_ptr = NULL;
		/*hash map uses the cycle list*/
		container_cache->table[i].hm_pre_ptr = &(container_cache->table[i]);
		container_cache->table[i].hm_next_ptr = &(container_cache->table[i]);
	}
	return container_cache;
}

void container_cache_destroy(struct _container_cache_table *container_cache)
{
	int i;
	struct _container_cache_node *cur, *next, *pre;
	cur = container_cache->lru.lru_next_ptr;
	
	
	for(i=0;i<container_cache->cached_num;i++){
		if(cur==&(container_cache->lru))
			break;
		next = cur->lru_next_ptr;
		free(cur->container_ptr);
		free(cur);
		cur = next;
	}
	free(container_cache);
}

void remove_from_container_cache(struct _container_cache_node *con_node)
{

	if((con_node ==NULL)||(con_node==&(GET_FPDD_DATA()->container_cache->lru)))
		return;
	/*remove from lru*/
	con_node->lru_next_ptr->lru_pre_ptr = con_node->lru_pre_ptr;
	con_node->lru_pre_ptr->lru_next_ptr = con_node->lru_next_ptr;

	/*remove from hash map*/
	con_node->hm_next_ptr->hm_pre_ptr = con_node->hm_pre_ptr;
	con_node->hm_pre_ptr->hm_next_ptr = con_node->hm_next_ptr;

	GET_FPDD_DATA()->container_cache->cached_num--;

	return;
}

uint64_t evict_from_container_cache(void)
{
	uint64_t container_name;
	struct _container_cache_node *tmp;
	tmp = GET_FPDD_DATA()->container_cache->lru.lru_pre_ptr;
	if(tmp==&(GET_FPDD_DATA()->container_cache->lru))
		return 0;
	container_name = tmp->container_name;

	/*free the content*/
	remove_from_container_cache(tmp);
	free(tmp->container_ptr);
	free(tmp);
	//result_log("evict: %lld, cache_num: %d\n", container_name,  GET_FPDD_DATA()->container_cache->cached_num);
	return container_name;
}
		

/*only the container data part pointer is added*/
int add_2_container_cache(char *container_ptr, uint64_t container_name)
{
	int pos,i;
	struct _container_cache_node *tmp;
	tmp = (struct _container_cache_node *)malloc(sizeof(struct _container_cache_node));
	tmp->container_name = container_name;
	tmp->container_ptr = container_ptr;
	//result_log("add to container cache: %lld, cache_num: %d\n", container_name,  GET_FPDD_DATA()->container_cache->cached_num);	
	GET_FPDD_DATA()->container_cache->cached_num++;
		
	/*check the cached num, if larger, do the evict first*/
	if(GET_FPDD_DATA()->container_cache->cached_num>CONTAINER_CACHE_NUM)
	{
		//result_log("cache num: %d, CONTAINER_CACHE_NUM: %d", GET_FPDD_DATA()->container_cache->cached_num, CONTAINER_CACHE_NUM);
		evict_from_container_cache();	
	}

	/*add to the LRU chain MRU end*/
	tmp->lru_next_ptr = GET_FPDD_DATA()->container_cache->lru.lru_next_ptr;
	tmp->lru_pre_ptr = GET_FPDD_DATA()->container_cache->lru.lru_next_ptr->lru_pre_ptr;
	GET_FPDD_DATA()->container_cache->lru.lru_next_ptr->lru_pre_ptr = tmp;
	GET_FPDD_DATA()->container_cache->lru.lru_next_ptr = tmp;
	
	/*add to the hashmap*/
	pos = container_name%CONTAINER_CACHE_BUCK;
	tmp->hm_next_ptr = GET_FPDD_DATA()->container_cache->table[pos].hm_next_ptr;
	tmp->hm_pre_ptr = GET_FPDD_DATA()->container_cache->table[pos].hm_next_ptr->hm_pre_ptr;
	GET_FPDD_DATA()->container_cache->table[pos].hm_next_ptr->hm_pre_ptr = tmp;
	GET_FPDD_DATA()->container_cache->table[pos].hm_next_ptr = tmp;
	
	return 0;
}



	
struct _container_cache_node * find_container_cache(uint64_t container_name)
{
	struct _container_cache_node *tmp, *return_tmp;
	int pos;
	return_tmp=NULL;
	pos = container_name%CONTAINER_CACHE_BUCK;
	tmp = &(GET_FPDD_DATA()->container_cache->table[pos]);
	while(tmp->hm_next_ptr!=&(GET_FPDD_DATA()->container_cache->table[pos]))
	{
		tmp = tmp->hm_next_ptr;
		if(tmp->container_name==container_name)
		{
			return_tmp = tmp;
			break;
		}
	}
	return return_tmp;
}



void move_mru_container_cache(struct _container_cache_node *tmp)
{
	int pos;
	remove_from_container_cache(tmp);
	
	GET_FPDD_DATA()->container_cache->cached_num++;
	/*add to the LRU chain MRU end*/
	tmp->lru_next_ptr = GET_FPDD_DATA()->container_cache->lru.lru_next_ptr;
	tmp->lru_pre_ptr = GET_FPDD_DATA()->container_cache->lru.lru_next_ptr->lru_pre_ptr;
	GET_FPDD_DATA()->container_cache->lru.lru_next_ptr->lru_pre_ptr = tmp;
	GET_FPDD_DATA()->container_cache->lru.lru_next_ptr = tmp;
	
	/*add to the hashmap*/
	pos = tmp->container_name%CONTAINER_CACHE_BUCK;
	tmp->hm_next_ptr = GET_FPDD_DATA()->container_cache->table[pos].hm_next_ptr;
	tmp->hm_pre_ptr = GET_FPDD_DATA()->container_cache->table[pos].hm_next_ptr->hm_pre_ptr;
	GET_FPDD_DATA()->container_cache->table[pos].hm_next_ptr->hm_pre_ptr = tmp;
	GET_FPDD_DATA()->container_cache->table[pos].hm_next_ptr = tmp;
	
	return;

}
		

	
	
	
		
	







	

