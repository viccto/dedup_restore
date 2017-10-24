/*
 ============================================================================
 Name        : nodecache.c
 Author      : Zhichao Cao
 Date        : 12/30/2016
 Copyright   : Your copyright notice
 Description : the restore functions of deduplication with look ahead return
				the buffer to the restoring main function
 ============================================================================
 */


#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>


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
#include "look_ahead.h"


#define READ_LEN (16 * 1024 * 1024)

bool can_do_look_ahead(void)
{
	return s_ptr_not_at_tail();
}


struct _look_ahead_table *look_ahead_table_init(void)
{
	struct _look_ahead_table *look_ahead_table;
	look_ahead_table = malloc(sizeof(struct _look_ahead_table));
	look_ahead_table->table_head = (struct _container_info *)malloc(sizeof(struct _container_info));
	look_ahead_table->table_head->container_next = NULL;
	look_ahead_table->table_head->chunk_next = NULL;
	look_ahead_table->table_head->container_name = 0;
	int i;
	for(i=0;i<CHUNK_INFO_HASH_NUM;i++)
	{
		look_ahead_table->chunk_info_hash[i].len = 0;
		look_ahead_table->chunk_info_hash[i].offset = 0;
		look_ahead_table->chunk_info_hash[i].container_name = 0;
		look_ahead_table->chunk_info_hash[i].chunk_name = 0;
		look_ahead_table->chunk_info_hash[i].a_buf_pos = 0;
		look_ahead_table->chunk_info_hash[i].next = &(look_ahead_table->chunk_info_hash[i]);
		look_ahead_table->chunk_info_hash[i].pre = &(look_ahead_table->chunk_info_hash[i]);
		look_ahead_table->chunk_info_hash[i].look_next = &(look_ahead_table->chunk_info_hash[i]);
		look_ahead_table->chunk_info_hash[i].look_pre = &(look_ahead_table->chunk_info_hash[i]);
	}
	return look_ahead_table;
}


void look_ahead_table_destroy(struct _look_ahead_table *table)
{
	free(table);
	return;
}

bool chunk_used_again_in_window(struct chunk_info * tmp_chunk)
{
	bool ret;
	struct chunk_info *tmp;
	if(tmp==NULL)
		return false;
	tmp = tmp_chunk->look_next;
	
	ret = false;
	while(tmp!=tmp_chunk)
	{
		if(tmp->chunk_name==tmp_chunk->chunk_name)
			ret = true;
		tmp = tmp->look_next;
	}
	return ret;
}


/*it is add to the tail of the hash list*/
void add_chunk_info_hash(struct chunk_info * tmp)
{
	if(tmp==NULL)
		return;
	int pos;
	pos = tmp->chunk_name%CHUNK_INFO_HASH_NUM;
	
	tmp->look_pre = GET_FPDD_DATA()->look_ahead_table->chunk_info_hash[pos].look_pre;
	tmp->look_next = GET_FPDD_DATA()->look_ahead_table->chunk_info_hash[pos].look_pre->look_next;
	GET_FPDD_DATA()->look_ahead_table->chunk_info_hash[pos].look_pre->look_next = tmp;
	GET_FPDD_DATA()->look_ahead_table->chunk_info_hash[pos].look_pre = tmp;
	return;
}

/*remove from the chunk info hash buck list*/
void remove_chunk_info_hash(struct chunk_info *tmp)
{
	if(tmp==NULL)
		return;
	
	tmp->look_next->look_pre = tmp->look_pre;
	tmp->look_pre->look_next = tmp->look_next;
	tmp->look_next = NULL;
	tmp->look_pre = NULL;
	return;
}



	
uint32_t assemble_buf(char *a_buf, struct metadata * mtdata, struct dedup_manager * dedup, uint64_t restored_num, FILE *fp)
{
#ifdef REGULAR_ASSEMBLE
	return regular_assemble(a_buf, mtdata, dedup, restored_num, fp);
#endif

#ifdef LOOK_AHEAD_ASSEMBLE
	return look_ahead_assemble(a_buf, mtdata, dedup, restored_num, fp);
#endif

}




/*regular assemble, assemble the chunk one by one to the a_buf*/
uint32_t regular_assemble(char *a_buf, struct metadata * mtdata, struct dedup_manager * dedup, uint64_t restored_num, FILE *fp)
{
	int i, a_size;
	char * buf;
	int ret;
	struct disk_hash_node node;
#ifdef COMPRESS
	char * uncom_buf;
	int uncom_len;
#endif

#ifdef COMPRESS
	uncom_buf = malloc (MAX_COMPRESS_LEN);
#endif
	buf = malloc(MAX_CHUNK_LEN);
	a_size = 0;

	for(i=0;i<restored_num;i++)
	{
		/*step1:  get the chunk from container*/

		get_chunk(buf, mtdata[i].offset, mtdata[i].len, mtdata[i].fingerprint, mtdata[i].container_name);
		GET_FPDD_DATA()->total_container_read++;

		/*step2: if compression is used, decompress and write to assembling buffer, or directly write to assembling buffer*/
		memcpy(a_buf+a_size, buf, mtdata[i].len);
		a_size = a_size + mtdata[i].len;	
	}
	fwrite(a_buf, 1, a_size, fp);
	return a_size;
}


/*look ahead assemble, assemble the chunk by look ahead, the container read is only one
 * for each container*/
uint32_t look_ahead_assemble(char *a_buf, struct metadata * mtdata, struct dedup_manager * dedup, uint64_t restored_num, FILE *fp)
{
	struct _container_info *table_head;
	table_head = (struct _container_info *)malloc(sizeof(struct _container_info));
	table_head->container_next = NULL;
	table_head->chunk_next = NULL;
	table_head->container_name = 0;
	uint32_t a_buf_size_r, a_buf_size;
	
	a_buf_size = construct_restore_table(mtdata, restored_num, table_head);

#ifdef CONTAINER_CACHE
	a_buf_size_r = restore_from_table_container_cache(restored_num, table_head, a_buf, fp);
#else
	#ifdef CHUNK_CACHE
		a_buf_size_r = restore_from_table_chunk_cache_full(restored_num, table_head, a_buf, fp);
	#else
		a_buf_size_r = restore_from_table(restored_num, table_head, a_buf, fp);
	#endif
#endif
	if(a_buf_size == a_buf_size_r)
	{
		free(table_head);
		return a_buf_size_r;
	}
	else
	{
		if(a_buf_size > a_buf_size_r)
			return a_buf_size;
		else
			return a_buf_size_r;
	}	
}


/*the look ahead table is always maintained, 
 *and it add a buffer size chunk infor
 * to the table*/
void add_to_look_ahead_window(struct metadata * mtdata, uint64_t restored_num, uint64_t look_ahead_count)
{
	struct _container_info *table_head;
	table_head = GET_FPDD_DATA()->look_ahead_table->table_head;
	int i;
	uint32_t a_buf_pos;
	bool inserted;
	struct _container_info *pos, *pos_pre;
	struct chunk_info * tmp_chunk;
	struct _container_info * tmp_container;

	a_buf_pos = 0;
	for(i=0;i<restored_num; i++)
	{
		pos_pre = table_head;
		pos = table_head->container_next;
		inserted = false;
		while(pos!=NULL)
		{
			if(pos->container_name==mtdata[i].container_name)
			{
				/*update the chunk_info value*/
				tmp_chunk = (struct chunk_info *)malloc(sizeof(struct chunk_info));
				tmp_chunk->offset = mtdata[i].offset;
				tmp_chunk->container_name = mtdata[i].container_name;
				tmp_chunk->chunk_name = mtdata[i].counter;
				tmp_chunk->a_buf_pos = a_buf_pos;
				tmp_chunk->len = mtdata[i].len;
				tmp_chunk->ab_offset = look_ahead_count;
				a_buf_pos = a_buf_pos+tmp_chunk->len;

				/*add to the list end*/
				tmp_chunk->next = pos->chunk_pre->next;
				tmp_chunk->pre = pos->chunk_pre;
				pos->chunk_pre->next = tmp_chunk;
				pos->chunk_pre = tmp_chunk;

				add_chunk_info_hash(tmp_chunk);
				
				inserted = true;
				break;
			}
			pos_pre = pos;
			pos = pos->container_next;
		}
		if(!inserted)
		{
			tmp_container = (struct _container_info *)malloc(sizeof(struct _container_info));
			tmp_container->container_name = mtdata[i].container_name;
			tmp_container->chunk_next = NULL;
			tmp_container->chunk_pre = NULL;

			tmp_container->container_next = pos_pre->container_next;
			pos_pre->container_next = tmp_container;
	
			tmp_chunk = (struct chunk_info *)malloc(sizeof(struct chunk_info));
			tmp_chunk->offset = mtdata[i].offset;
			tmp_chunk->container_name = mtdata[i].container_name;
			tmp_chunk->chunk_name = mtdata[i].counter;
			tmp_chunk->a_buf_pos = a_buf_pos;
			tmp_chunk->len = mtdata[i].len;
			tmp_chunk->ab_offset = look_ahead_count;
			a_buf_pos = a_buf_pos+tmp_chunk->len;

			tmp_chunk->next = NULL;
			tmp_chunk->pre = NULL;
			tmp_container->chunk_pre= tmp_chunk;
			tmp_container->chunk_next = tmp_chunk;

			add_chunk_info_hash(tmp_chunk);
		}
	
	}
	return;
}


/*
 * restore one buffer size data from the table(if smaller
 * restore smaller) finally write to the file
 *during the restore, first, starting from the look 
 * ahead table, if mach assemble_count, do restore, at the 
 * same time, check if the chunk will be used in the future
 * if so, do nothing, if not, move after the seperate_ptr.
 * for the chunk not cached, read in the container. begin
 * from the container list, to end. if not used in future, 
 */

int restore_assemble_buf_write(char *a_buf, uint64_t dedup_num, uint64_t assemble_count,FILE *fp)
{
	struct _container_info *table_head;
	table_head = GET_FPDD_DATA()->look_ahead_table->table_head;
	struct _container_info *tmp_con, *pre_con;
	struct chunk_info * tmp_chunk, *chunk_tmp;
	struct _chunk_cache_node *cache_chunk, *tmp_cache_chunk;
	char *container_buf;
	char *buf;
	uint32_t a_buf_size;
	int count, ab_offset, i;
	bool not_cached, get_container;

	buf = malloc(MAX_CHUNK_LEN);
	container_buf = malloc(MAX_CONTAINER_DATA);
	a_buf_size = 0;
	count = 0;
	not_cached = true;

	pre_con = table_head;
	tmp_con =  table_head->container_next;
	//result_log("the turns in the LAW: %d, and ab_offset: %d\n",i, ab_offset);
	while(tmp_con!=NULL)
	{
		//result_log("count: %d, container: %lld\n", count, tmp_con->container_name);		
		tmp_chunk = tmp_con->chunk_next;
		get_container = false;
		if((tmp_chunk!=NULL)&&(tmp_chunk->ab_offset != assemble_count))
		{
			pre_con = tmp_con;
			tmp_con = tmp_con->container_next;
			continue;
		}

		while(tmp_chunk!=NULL)
		{
			/*the caching steps is finished, data is in the cache_chunk*/
			if(tmp_chunk->ab_offset != assemble_count)
				break;

				/*check the cache first*/
			cache_chunk = find_chunk_cache(tmp_chunk->chunk_name);
			if(cache_chunk==NULL)
			{
					/*chunk not in the cache, also not in the
					 * container_buf need to read from disk*/
				if(get_container)
				{
						/*currently the container is read in*/
					cache_chunk = (struct _chunk_cache_node *)malloc(sizeof(struct _chunk_cache_node));
					cache_chunk->chunk_ptr = malloc(MAX_CHUNK_LEN);
					cache_chunk->chunk_name = tmp_chunk->chunk_name;
					memcpy(cache_chunk->chunk_ptr, container_buf+tmp_chunk->offset,tmp_chunk->len);
					if(chunk_used_again_in_window(tmp_chunk))
						add_2_chunk_cache_head(cache_chunk);
						//add_2_chunk_cache_before_s_prt(cache_chunk);
					else
						add_2_chunk_cache_after_s_prt(cache_chunk);

				}
				else
				{
						/*read in the container*/
					GET_FPDD_DATA()->cache_miss++;
					GET_FPDD_DATA()->total_container_read++;
					read_container_to_buf(container_buf, tmp_con->container_name);
					get_container = true;
						
					/*add the rest chunks to cache*/
					chunk_tmp = tmp_chunk->next;
						
					while(chunk_tmp!=NULL)
					{
						cache_chunk = find_chunk_cache(chunk_tmp->chunk_name);
						if(cache_chunk==NULL)
						{
							cache_chunk = (struct _chunk_cache_node *)malloc(sizeof(struct _chunk_cache_node));
							cache_chunk->chunk_ptr = malloc(MAX_CHUNK_LEN);
							cache_chunk->chunk_name = chunk_tmp->chunk_name;
							memcpy(cache_chunk->chunk_ptr, container_buf+chunk_tmp->offset,chunk_tmp->len);
							add_2_chunk_cache_head(cache_chunk);
								
						}
						chunk_tmp = chunk_tmp->next;
					}
					cache_chunk = (struct _chunk_cache_node *)malloc(sizeof(struct _chunk_cache_node));
					cache_chunk->chunk_ptr = malloc(MAX_CHUNK_LEN);
					cache_chunk->chunk_name = tmp_chunk->chunk_name;
					memcpy(cache_chunk->chunk_ptr, container_buf+tmp_chunk->offset,tmp_chunk->len);
					add_2_chunk_cache_head(cache_chunk);
	
				}
			}
			else
			{
				GET_FPDD_DATA()->cache_hit++;
				if(chunk_used_again_in_window(tmp_chunk))
					move_2_chunk_cache_head(cache_chunk);
						//add_2_chunk_cache_before_s_prt(cache_chunk, tmp_chunk->chunk_name);
				else
					move_2_chunk_cache_after_s_prt(cache_chunk);
			}


			/*the chunk belongs to this assembly buffer*/
			memcpy(a_buf+a_buf_size, cache_chunk->chunk_ptr,tmp_chunk->len);
			a_buf_size = a_buf_size+tmp_chunk->len;
			GET_FPDD_DATA()->total_restore_size +=tmp_chunk->len;

			tmp_con->chunk_next = tmp_chunk->next;
			remove_chunk_info_hash(tmp_chunk);
			free(tmp_chunk);
			tmp_chunk = tmp_con->chunk_next;
		}

			/*the container will be used in the future AB*/
		if(tmp_con->chunk_next!=NULL)
		{
			pre_con = tmp_con;
			tmp_con = tmp_con->container_next;
		}			
		else
		{
			pre_con->container_next = tmp_con->container_next;
			free(tmp_con);
			tmp_con = pre_con->container_next;
		}
	}
#ifdef CONTENT_FREE_RESTORE
	fseek(fp, 0, SEEK_SET);
	fwrite(a_buf, 1, a_buf_size, fp);
#else	
	fwrite(a_buf, 1, a_buf_size, fp);
#endif

	//result_log("write to the file: %ld\n",a_buf_size);

	free(buf);
	free(container_buf);
	return a_buf_size;
}

int restore_assemble_pipe_line_buf_write(char *a_buf, uint64_t dedup_num, uint64_t assemble_count,FILE *fp, int cache_mode)
{
	if(cache_mode==1)
		return restore_assemble_pipe_line_container_cache(a_buf, ASSEMBLE_BUF_SIZE, assemble_count, fp);
	if(cache_mode==2)
		return restore_assemble_pipe_line_chunk_cache(a_buf, ASSEMBLE_BUF_SIZE, assemble_count, fp);
}

uint32_t restore_assemble_pipe_line_container_cache(char *a_buf, uint64_t dedup_num, uint64_t assemble_count,FILE *fp)
{
	struct _container_info *table_head;
	table_head = GET_FPDD_DATA()->look_ahead_table->table_head;
	struct _container_info *tmp_con, *pre_con;
	struct chunk_info * tmp_chunk;
	struct _container_cache_node *cache_container;
	char *container_buf;
	char *buf;
	uint32_t a_buf_size, a_buf_size_t;
	int count, ab_offset, i;
	bool not_cached;

	buf = malloc(MAX_CHUNK_LEN);
	a_buf_size = 0;
	a_buf_size_t = 0;
	count = 0;
	ab_offset = 0;
	not_cached = true;

	pre_con = table_head;
	tmp_con =  table_head->container_next;
		//result_log("the turns in the LAW: %d, and ab_offset: %d\n",i, ab_offset);
	while(tmp_con!=NULL)
	{
			//result_log("count: %d, container: %lld\n", count, tmp_con->container_name);
				
		tmp_chunk = tmp_con->chunk_next;
		if((tmp_chunk!=NULL)&&(tmp_chunk->ab_offset != assemble_count))
		{
			pre_con = tmp_con;
			tmp_con = tmp_con->container_next;
			continue;
		}
		cache_container = find_container_cache(tmp_con->container_name);
		if(cache_container==NULL)
		{
			GET_FPDD_DATA()->cache_miss++;
			container_buf = malloc(MAX_CONTAINER_DATA);
			GET_FPDD_DATA()->total_container_read++;
			read_container_to_buf(container_buf, tmp_con->container_name);
			//add_2_container_cache(container_buf, tmp_con->container_name);
			not_cached = true;
		}
		else
		{
			container_buf = cache_container->container_ptr;
			not_cached = false;
			GET_FPDD_DATA()->cache_hit++;
				//result_log("container cache hit: %lld\n", tmp_con->container_name);
		}
		while(tmp_chunk!=NULL)
		{

			if(tmp_chunk->ab_offset != assemble_count)
				break;
			memcpy(a_buf+a_buf_size, container_buf+tmp_chunk->offset,tmp_chunk->len);
			a_buf_size = a_buf_size+tmp_chunk->len;
			GET_FPDD_DATA()->total_restore_size +=tmp_chunk->len;

			tmp_con->chunk_next = tmp_chunk->next;
			remove_chunk_info_hash(tmp_chunk);
			free(tmp_chunk);
			tmp_chunk = tmp_con->chunk_next;

			count++;
		}
			/*the container will be used in the future AB*/
		if(tmp_chunk!=NULL)
		{
			if(not_cached)
			{	
				add_2_container_cache(container_buf, tmp_con->container_name);
			}
			else
			{
					/*cached, move to the MRU side*/
				move_mru_container_cache(cache_container);
			}
			pre_con = tmp_con;
			tmp_con = tmp_con->container_next;
		}			
		else
		{
			pre_con->container_next = tmp_con->container_next;
			if(not_cached)
			{
				add_2_container_cache(container_buf, tmp_con->container_name);
			}
			free(tmp_con);
			tmp_con = pre_con->container_next;
		}
	}
		
#ifdef CONTENT_FREE_RESTORE
	fseek(fp, 0, SEEK_SET);
	//fwrite(a_buf, 1, a_buf_size, fp);
#else	
	fwrite(a_buf, 1, a_buf_size, fp);
#endif
		//result_log("write to the file: %ld\n",a_buf_size);
	free(buf);
	return a_buf_size;
}



int restore_assemble_pipe_line_chunk_cache(char *a_buf, uint64_t dedup_num, uint64_t assemble_count,FILE *fp)
{
	struct _container_info *table_head;
	table_head = GET_FPDD_DATA()->look_ahead_table->table_head;
	struct _container_info *tmp_con, *pre_con;
	struct chunk_info * tmp_chunk, *chunk_tmp;
	struct _chunk_cache_node *cache_chunk, *tmp_cache_chunk;
	char *container_buf;
	char *buf;
	uint32_t a_buf_size;
	int count, ab_offset, i;
	bool not_cached, get_container;

	buf = malloc(MAX_CHUNK_LEN);
	container_buf = malloc(MAX_CONTAINER_DATA);
	a_buf_size = 0;
	count = 0;
	not_cached = true;

	pre_con = table_head;
	tmp_con =  table_head->container_next;
	//result_log("the turns in the LAW: %d, and ab_offset: %d\n",i, ab_offset);
	while(tmp_con!=NULL)
	{
		//result_log("count: %d, container: %lld\n", count, tmp_con->container_name);		
		tmp_chunk = tmp_con->chunk_next;
		get_container = false;
		if((tmp_chunk!=NULL)&&(tmp_chunk->ab_offset != assemble_count))
		{
			pre_con = tmp_con;
			tmp_con = tmp_con->container_next;
			continue;
		}

		while(tmp_chunk!=NULL)
		{
			/*the caching steps is finished, data is in the cache_chunk*/
			if(tmp_chunk->ab_offset != assemble_count)
				break;

				/*check the cache first*/
			cache_chunk = find_chunk_cache(tmp_chunk->chunk_name);
			if(cache_chunk==NULL)
			{
					/*chunk not in the cache, also not in the
					 * container_buf need to read from disk*/
				if(get_container)
				{
						/*currently the container is read in*/
					cache_chunk = (struct _chunk_cache_node *)malloc(sizeof(struct _chunk_cache_node));
					cache_chunk->chunk_ptr = malloc(MAX_CHUNK_LEN);
					cache_chunk->chunk_name = tmp_chunk->chunk_name;
					memcpy(cache_chunk->chunk_ptr, container_buf+tmp_chunk->offset,tmp_chunk->len);
					if(chunk_used_again_in_window(tmp_chunk))
						add_2_chunk_cache_head(cache_chunk);

				}
				else
				{
						/*read in the container*/
					GET_FPDD_DATA()->cache_miss++;
					GET_FPDD_DATA()->total_container_read++;
					read_container_to_buf(container_buf, tmp_con->container_name);
					get_container = true;
						
					/*add the rest chunks to cache*/
					chunk_tmp = tmp_chunk->next;
						
					while(chunk_tmp!=NULL)
					{
						cache_chunk = find_chunk_cache(chunk_tmp->chunk_name);
						if(cache_chunk==NULL)
						{
							cache_chunk = (struct _chunk_cache_node *)malloc(sizeof(struct _chunk_cache_node));
							cache_chunk->chunk_ptr = malloc(MAX_CHUNK_LEN);
							cache_chunk->chunk_name = chunk_tmp->chunk_name;
							memcpy(cache_chunk->chunk_ptr, container_buf+chunk_tmp->offset,chunk_tmp->len);
							add_2_chunk_cache_head(cache_chunk);
								
						}
						chunk_tmp = chunk_tmp->next;
					}
					cache_chunk = (struct _chunk_cache_node *)malloc(sizeof(struct _chunk_cache_node));
					cache_chunk->chunk_ptr = malloc(MAX_CHUNK_LEN);
					cache_chunk->chunk_name = tmp_chunk->chunk_name;
					memcpy(cache_chunk->chunk_ptr, container_buf+tmp_chunk->offset,tmp_chunk->len);
					add_2_chunk_cache_head(cache_chunk);
	
				}
			}
			else
			{
				GET_FPDD_DATA()->cache_hit++;
				if(chunk_used_again_in_window(tmp_chunk))
					move_2_chunk_cache_head(cache_chunk);
			}


			/*the chunk belongs to this assembly buffer*/
			memcpy(a_buf+a_buf_size, cache_chunk->chunk_ptr,tmp_chunk->len);
			a_buf_size = a_buf_size+tmp_chunk->len;
			GET_FPDD_DATA()->total_restore_size +=tmp_chunk->len;

			tmp_con->chunk_next = tmp_chunk->next;
			remove_chunk_info_hash(tmp_chunk);
			free(tmp_chunk);
			tmp_chunk = tmp_con->chunk_next;
		}

			/*the container will be used in the future AB*/
		if(tmp_con->chunk_next!=NULL)
		{
			pre_con = tmp_con;
			tmp_con = tmp_con->container_next;
		}			
		else
		{
			pre_con->container_next = tmp_con->container_next;
			free(tmp_con);
			tmp_con = pre_con->container_next;
		}
	}
		
#ifdef CONTENT_FREE_RESTORE
	fseek(fp, 0, SEEK_SET);
	//fwrite(a_buf, 1, a_buf_size, fp);
#else	
	fwrite(a_buf, 1, a_buf_size, fp);
#endif
	//result_log("write to the file: %ld\n",a_buf_size);

	free(buf);
	free(container_buf);
	return a_buf_size;
}





/*construct the restore table, 
 * it is actually the look ahead window,
 * all the pointers are malloced in it except the table head*/
uint32_t construct_restore_table(struct metadata * mtdata, uint64_t restored_num, struct _container_info *table_head)
{
	int i, ab_offset, count;
	uint32_t a_buf_pos,a_buf_pos_t;
	bool inserted;
	struct _container_info *pos, *pos_pre;
	struct chunk_info * tmp_chunk;
	struct _container_info * tmp_container;

	a_buf_pos = 0;
	a_buf_pos_t =0;
	ab_offset = 0;
	count = 0;
	for(i=0;i<restored_num; i++)
	{
		pos_pre = table_head;
		pos = table_head->container_next;
		inserted = false;
		while(pos!=NULL)
		{
			if(pos->container_name==mtdata[i].container_name)
			{
				tmp_chunk = (struct chunk_info *)malloc(sizeof(struct chunk_info));
				tmp_chunk->offset = mtdata[i].offset;
				tmp_chunk->container_name = mtdata[i].container_name;
				tmp_chunk->chunk_name = mtdata[i].counter;
				tmp_chunk->a_buf_pos = a_buf_pos;
				tmp_chunk->len = mtdata[i].len;
				tmp_chunk->ab_offset = ab_offset;
				a_buf_pos = a_buf_pos+tmp_chunk->len;

				tmp_chunk->next = pos->chunk_pre->next;
				tmp_chunk->pre = pos->chunk_pre;
				pos->chunk_pre->next = tmp_chunk;
				pos->chunk_pre = tmp_chunk;
	
				add_chunk_info_hash(tmp_chunk);
				
				inserted = true;
				break;
			}
			pos_pre = pos;
			pos = pos->container_next;
		}
		if(!inserted)
		{
			tmp_container = (struct _container_info *)malloc(sizeof(struct _container_info));
			tmp_container->container_name = mtdata[i].container_name;
			tmp_container->chunk_next = NULL;
			tmp_container->chunk_pre = NULL;

			tmp_container->container_next = pos_pre->container_next;
			pos_pre->container_next = tmp_container;
	
			tmp_chunk = (struct chunk_info *)malloc(sizeof(struct chunk_info));
			tmp_chunk->offset = mtdata[i].offset;
			tmp_chunk->container_name = mtdata[i].container_name;
			tmp_chunk->chunk_name = mtdata[i].counter;
			tmp_chunk->a_buf_pos = a_buf_pos;
			tmp_chunk->len = mtdata[i].len;
			tmp_chunk->ab_offset = ab_offset;
			a_buf_pos = a_buf_pos+tmp_chunk->len;

			tmp_chunk->next = NULL;
			tmp_chunk->pre = NULL;
			tmp_container->chunk_pre= tmp_chunk;
			tmp_container->chunk_next = tmp_chunk;

			add_chunk_info_hash(tmp_chunk);
		}

		count++;
		/*write the ab_offset*/
		if(count>=ASSEMBLE_BUF_SIZE)
		{
			count = 0;
			ab_offset++;
			a_buf_pos_t = a_buf_pos_t+a_buf_pos;
			a_buf_pos = 0;
		}
	
	}
	return a_buf_pos_t;
}

/*restore the a_buf from the table created by construct_restore_table
 * assume that a_buf is alread malloced
 * here, the assemble buffer size is the same as the look ahead size*/
uint32_t restore_from_table(uint64_t restored_num, struct _container_info *table_head, char *a_buf, FILE *fp)
{
	struct _container_info *tmp_con;
	struct chunk_info * tmp_chunk;
	char *container_buf;
	char *buf;
	uint32_t a_buf_size;

	buf = malloc(MAX_CHUNK_LEN);
	container_buf = malloc(MAX_CONTAINER_DATA);
	a_buf_size = 0;
	tmp_con =  table_head->container_next;

	while(tmp_con!=NULL)
	{
		tmp_chunk = tmp_con->chunk_next;
		read_container_to_buf(container_buf, tmp_con->container_name);
		GET_FPDD_DATA()->total_container_read++;
		while(tmp_chunk!=NULL)
		{
			memcpy(a_buf+a_buf_size, container_buf+tmp_chunk->offset,tmp_chunk->len);
			a_buf_size = a_buf_size+tmp_chunk->len;
			GET_FPDD_DATA()->total_restore_size +=tmp_chunk->len;

			tmp_con->chunk_next = tmp_chunk->next;
			remove_chunk_info_hash(tmp_chunk);
			free(tmp_chunk);
			tmp_chunk = tmp_con->chunk_next;
		}
		table_head->container_next = tmp_con->container_next;
		free(tmp_con);
		tmp_con = table_head->container_next;
	}
	free(container_buf);
	free(buf);
#ifdef CONTENT_FREE_RESTORE
	fseek(fp, 0, SEEK_SET);
	fwrite(a_buf, 1, a_buf_size, fp);
#else	
	fwrite(a_buf, 1, a_buf_size, fp);
#endif
	return a_buf_size;
}
			

uint32_t restore_from_table_container_cache(uint64_t restored_num, struct _container_info *table_head, char *a_buf, FILE *fp)
{
	struct _container_info *tmp_con, *pre_con;
	struct chunk_info * tmp_chunk;
	struct _container_cache_node *cache_container;
	char *container_buf;
	char *buf;
	uint32_t a_buf_size, a_buf_size_t;
	int count, ab_offset, i;
	bool not_cached;

	buf = malloc(MAX_CHUNK_LEN);
	a_buf_size = 0;
	a_buf_size_t = 0;
	count = 0;
	ab_offset = 0;
	not_cached = true;

	for(i=0;i<LOOK_AHEAD_TIMES; i++)
	{
		a_buf_size = 0;
		pre_con = table_head;
		tmp_con =  table_head->container_next;
		if(tmp_con == NULL)
			break;
		//result_log("the turns in the LAW: %d, and ab_offset: %d\n",i, ab_offset);
		while(tmp_con!=NULL)
		{
			//result_log("count: %d, container: %lld\n", count, tmp_con->container_name);
			if(count>=ASSEMBLE_BUF_SIZE)
			{
				count = 0;
				ab_offset++;
				break;
			}
				
			tmp_chunk = tmp_con->chunk_next;
			cache_container = find_container_cache(tmp_con->container_name);
			if(cache_container==NULL)
			{
				GET_FPDD_DATA()->cache_miss++;
				container_buf = malloc(MAX_CONTAINER_DATA);
				GET_FPDD_DATA()->total_container_read++;
				read_container_to_buf(container_buf, tmp_con->container_name);
				not_cached = true;
				
			}
			else
			{
				container_buf = cache_container->container_ptr;
				not_cached = false;
				GET_FPDD_DATA()->cache_hit++;
				//result_log("container cache hit: %lld\n", tmp_con->container_name);
			}
			while(tmp_chunk!=NULL)
			{

				if(tmp_chunk->ab_offset != ab_offset)
					break;
				memcpy(a_buf+a_buf_size, container_buf+tmp_chunk->offset,tmp_chunk->len);
				a_buf_size = a_buf_size+tmp_chunk->len;
				GET_FPDD_DATA()->total_restore_size +=tmp_chunk->len;

				tmp_con->chunk_next = tmp_chunk->next;
				remove_chunk_info_hash(tmp_chunk);
				free(tmp_chunk);
				tmp_chunk = tmp_con->chunk_next;

				count++;
			}
			/*the container will be used in the future AB*/
			if(tmp_chunk!=NULL)
			{
				if(not_cached)
					add_2_container_cache(container_buf, tmp_con->container_name);
				else
				{
					/*cached, move to the MRU side*/
					move_mru_container_cache(cache_container);
				}
				pre_con = tmp_con;
				tmp_con = tmp_con->container_next;
			}			
			else
			{
				pre_con->container_next = tmp_con->container_next;
				if(not_cached)
				{
					add_2_container_cache(container_buf, tmp_con->container_name);
				}
				free(tmp_con);
				tmp_con = pre_con->container_next;
			}
		}
		
#ifdef CONTENT_FREE_RESTORE
	fseek(fp, 0, SEEK_SET);
	fwrite(a_buf, 1, a_buf_size, fp);
#else	
	fwrite(a_buf, 1, a_buf_size, fp);
#endif
		//result_log("write to the file: %ld\n",a_buf_size);
		a_buf_size_t = a_buf_size_t+a_buf_size;
	}
	free(buf);
	return a_buf_size_t;
}





uint32_t restore_from_table_chunk_cache_full(uint64_t restored_num, struct _container_info *table_head, char *a_buf, FILE *fp)
{
	struct _container_info *tmp_con, *pre_con;
	struct chunk_info * tmp_chunk, *chunk_tmp;
	struct _chunk_cache_node *cache_chunk, *tmp_cache_chunk;
	char *container_buf;
	char *chunk_buf;
	char *buf;
	uint32_t a_buf_size, a_buf_size_t;
	int count, ab_offset, i;
	bool not_cached;
	bool get_container;
	bool back_to_begin;

	buf = malloc(MAX_CHUNK_LEN);
	container_buf = malloc(MAX_CONTAINER_DATA);
	a_buf_size = 0;
	a_buf_size_t = 0;
	count = 0;
	ab_offset = 0;
	not_cached = true;

	tmp_con =  table_head->container_next;
	/*
	while(tmp_con!=NULL)
	{
		tmp_chunk = tmp_con->chunk_next;
		while(tmp_chunk!=NULL)
		{
			//check the cache first
			cache_chunk = find_chunk_cache(tmp_chunk->chunk_name);
			if(cache_chunk==NULL)
			{
				if(get_container)
				{
					//currently the container is read in
					cache_chunk = (struct _chunk_cache_node *)malloc(sizeof(struct _chunk_cache_node));
					cache_chunk->chunk_ptr = malloc(MAX_CHUNK_LEN);
					cache_chunk->chunk_name = tmp_chunk->chunk_name;
					memcpy(cache_chunk->chunk_ptr, container_buf+tmp_chunk->offset,tmp_chunk->len);
					add_2_chunk_cache_head(cache_chunk);
				}
				else
				{
					GET_FPDD_DATA()->total_container_read++;
					read_container_to_buf(container_buf, tmp_con->container_name);
					cache_chunk = (struct _chunk_cache_node *)malloc(sizeof(struct _chunk_cache_node));
					cache_chunk->chunk_ptr = malloc(MAX_CHUNK_LEN);
					cache_chunk->chunk_name = tmp_chunk->chunk_name;
					memcpy(cache_chunk->chunk_ptr, container_buf+tmp_chunk->offset,tmp_chunk->len);
					add_2_chunk_cache_head(cache_chunk);
					get_container = true;
				}
			}
			tmp_chunk = tmp_chunk->next;
		}
		tmp_con = tmp_con->container_next;
	}
	*/

	for(i=0;i<LOOK_AHEAD_TIMES; i++)
	{
		a_buf_size = 0;
		pre_con = table_head;
		tmp_con =  table_head->container_next;
		if(tmp_con == NULL)
			break;
		back_to_begin = false;
		//result_log("the turns in the LAW: %d, and ab_offset: %d\n",i, ab_offset);
		while((tmp_con!=NULL)&&(!back_to_begin))
		{
			//result_log("count: %d, container: %lld\n", count, tmp_con->container_name);
			tmp_chunk = tmp_con->chunk_next;
			get_container = false;

			while(tmp_chunk!=NULL)
			{
				/*check the cache first*/
				cache_chunk = find_chunk_cache(tmp_chunk->chunk_name);
				if(cache_chunk==NULL)
				{
					if((tmp_chunk->ab_offset == ab_offset)&&(!back_to_begin))
						GET_FPDD_DATA()->cache_miss++;
					/*chunk not in the cache, also not in the
					 * container_buf need to read from disk*/
					if(get_container)
					{
						/*currently the container is read in*/
						cache_chunk = (struct _chunk_cache_node *)malloc(sizeof(struct _chunk_cache_node));
						cache_chunk->chunk_ptr = malloc(MAX_CHUNK_LEN);
						cache_chunk->chunk_name = tmp_chunk->chunk_name;
						memcpy(cache_chunk->chunk_ptr, container_buf+tmp_chunk->offset,tmp_chunk->len);
						add_2_chunk_cache_head(cache_chunk);

					}
					else
					{
						/*read in the container*/
						GET_FPDD_DATA()->total_container_read++;
						read_container_to_buf(container_buf, tmp_con->container_name);
						get_container = true;
						
						/*add the chunk to cache*/
						chunk_tmp = tmp_chunk->next;
						//result_log("cache the chunk in this run: .........container: %lld\n", tmp_con->container_name);
						while(chunk_tmp!=NULL)
						{
							cache_chunk = find_chunk_cache(chunk_tmp->chunk_name);
							if(cache_chunk==NULL)
							{
								cache_chunk = (struct _chunk_cache_node *)malloc(sizeof(struct _chunk_cache_node));
								cache_chunk->chunk_ptr = malloc(MAX_CHUNK_LEN);
								cache_chunk->chunk_name = chunk_tmp->chunk_name;
								memcpy(cache_chunk->chunk_ptr, container_buf+chunk_tmp->offset,chunk_tmp->len);
								add_2_chunk_cache_head(cache_chunk);
								
							}
							chunk_tmp = chunk_tmp->next;
						}
						cache_chunk = (struct _chunk_cache_node *)malloc(sizeof(struct _chunk_cache_node));
						cache_chunk->chunk_ptr = malloc(MAX_CHUNK_LEN);
						cache_chunk->chunk_name = tmp_chunk->chunk_name;
						memcpy(cache_chunk->chunk_ptr, container_buf+tmp_chunk->offset,tmp_chunk->len);
						add_2_chunk_cache_head(cache_chunk);
					}
	
				}
				else
				{
					if((tmp_chunk->ab_offset == ab_offset)&&(!back_to_begin))
						GET_FPDD_DATA()->cache_hit++;
					chunk_cache_move_mru(cache_chunk);
				}
				if((tmp_chunk->ab_offset == ab_offset)&&(!back_to_begin))
				{
					/*the chunk belongs to this assembly buffer*/
					memcpy(a_buf+a_buf_size, cache_chunk,tmp_chunk->len);
					a_buf_size = a_buf_size+tmp_chunk->len;
					GET_FPDD_DATA()->total_restore_size +=tmp_chunk->len;

					tmp_con->chunk_next = tmp_chunk->next;
					remove_chunk_info_hash(tmp_chunk);
					free(tmp_chunk);
					tmp_chunk = tmp_con->chunk_next;
					count++;
					if(count>=ASSEMBLE_BUF_SIZE)
					{
						count = 0;
						back_to_begin = true;
						break;
					}
				}
				else
				{
					tmp_chunk = tmp_chunk->next;
				}
			}
			/*the container will be used in the future AB*/
			if(tmp_con->chunk_next!=NULL)
			{
				pre_con = tmp_con;
				tmp_con = tmp_con->container_next;
			}			
			else
			{
				pre_con->container_next = tmp_con->container_next;
				free(tmp_con);
				tmp_con = pre_con->container_next;
			}
		}
		
	#ifdef CONTENT_FREE_RESTORE
		fseek(fp, 0, SEEK_SET);
		fwrite(a_buf, 1, a_buf_size, fp);
	#else	
		fwrite(a_buf, 1, a_buf_size, fp);
	#endif
		ab_offset++;
		//result_log("write to the file: %ld\n",a_buf_size);
		a_buf_size_t = a_buf_size_t+a_buf_size;
	}
	free(buf);
	free(container_buf);
	return a_buf_size_t;
}


uint32_t restore_from_table_chunk_cache_batch(uint64_t restored_num, struct _container_info *table_head, char *a_buf, FILE *fp, int look_ahead_times, int *current_pcache)
{
	struct _container_info *tmp_con, *pre_con;
	struct chunk_info * tmp_chunk, *chunk_tmp;
	struct _chunk_cache_node *cache_chunk, *tmp_cache_chunk;
	char *container_buf;
	char *chunk_buf;
	char *buf;
	uint32_t a_buf_size, a_buf_size_t;
	int count, ab_offset, i;
	bool not_cached;
	bool get_container;
	bool back_to_begin;

	buf = malloc(MAX_CHUNK_LEN);
	container_buf = malloc(MAX_CONTAINER_DATA);
	a_buf_size = 0;
	a_buf_size_t = 0;
	count = 0;
	ab_offset = 0;
	not_cached = true;

	tmp_con =  table_head->container_next;
	
	/*
	while(tmp_con!=NULL)
	{
		tmp_chunk = tmp_con->chunk_next;
		while(tmp_chunk!=NULL)
		{
			if(chunk_cache_pcache_size()<=1)
				break;
			//check the cache first
			cache_chunk = find_chunk_cache(tmp_chunk->chunk_name);
			if(cache_chunk==NULL)
			{
				if(get_container)
				{
					//currently the container is read in
					cache_chunk = (struct _chunk_cache_node *)malloc(sizeof(struct _chunk_cache_node));
					cache_chunk->chunk_ptr = malloc(MAX_CHUNK_LEN);
					cache_chunk->chunk_name = tmp_chunk->chunk_name;
					memcpy(cache_chunk->chunk_ptr, container_buf+tmp_chunk->offset,tmp_chunk->len);
					add_2_chunk_cache_head(cache_chunk);
				}
				else
				{
					GET_FPDD_DATA()->total_container_read++;
					read_container_to_buf(container_buf, tmp_con->container_name);
					cache_chunk = (struct _chunk_cache_node *)malloc(sizeof(struct _chunk_cache_node));
					cache_chunk->chunk_ptr = malloc(MAX_CHUNK_LEN);
					cache_chunk->chunk_name = tmp_chunk->chunk_name;
					memcpy(cache_chunk->chunk_ptr, container_buf+tmp_chunk->offset,tmp_chunk->len);
					add_2_chunk_cache_head(cache_chunk);
					get_container = true;
				}
			}
			tmp_chunk = tmp_chunk->next;
		}
		tmp_con = tmp_con->container_next;
	}
	*/
	*current_pcache =  chunk_cache_pcache_size();
	
	for(i=0;i<look_ahead_times; i++)
	{
		a_buf_size = 0;
		pre_con = table_head;
		tmp_con =  table_head->container_next;
		if(tmp_con == NULL)
			break;
		back_to_begin = false;
		//result_log("the turns in the LAW: %d, and ab_offset: %d\n",i, ab_offset);
		while((tmp_con!=NULL)&&(!back_to_begin))
		{
			tmp_chunk = tmp_con->chunk_next;
			get_container = false;

			while(tmp_chunk!=NULL)
			{
				/*check the cache first*/
				cache_chunk = find_chunk_cache(tmp_chunk->chunk_name);
				if(cache_chunk==NULL)
				{
					if((tmp_chunk->ab_offset == ab_offset)&&(!back_to_begin))
						GET_FPDD_DATA()->cache_miss++;
					/*chunk not in the cache, also not in the
					 * container_buf need to read from disk*/
					if(get_container)
					{
						/*currently the container is read in*/
						cache_chunk = (struct _chunk_cache_node *)malloc(sizeof(struct _chunk_cache_node));
						cache_chunk->chunk_ptr = malloc(MAX_CHUNK_LEN);
						cache_chunk->chunk_name = tmp_chunk->chunk_name;
						memcpy(cache_chunk->chunk_ptr, container_buf+tmp_chunk->offset,tmp_chunk->len);
						add_2_chunk_cache_head(cache_chunk);

					}
					else
					{
						/*read in the container*/
						GET_FPDD_DATA()->total_container_read++;
						read_container_to_buf(container_buf, tmp_con->container_name);
						get_container = true;
						
						/*add the chunk to cache*/
						chunk_tmp = tmp_chunk->next;
						//result_log("cache the chunk in this run: .........container: %lld\n", tmp_con->container_name);
						while(chunk_tmp!=NULL)
						{
							cache_chunk = find_chunk_cache(chunk_tmp->chunk_name);
							if(cache_chunk==NULL)
							{
								cache_chunk = (struct _chunk_cache_node *)malloc(sizeof(struct _chunk_cache_node));
								cache_chunk->chunk_ptr = malloc(MAX_CHUNK_LEN);
								cache_chunk->chunk_name = chunk_tmp->chunk_name;
								memcpy(cache_chunk->chunk_ptr, container_buf+chunk_tmp->offset,chunk_tmp->len);
								add_2_chunk_cache_head(cache_chunk);
								
							}
							chunk_tmp = chunk_tmp->next;
						}
						cache_chunk = (struct _chunk_cache_node *)malloc(sizeof(struct _chunk_cache_node));
						cache_chunk->chunk_ptr = malloc(MAX_CHUNK_LEN);
						cache_chunk->chunk_name = tmp_chunk->chunk_name;
						memcpy(cache_chunk->chunk_ptr, container_buf+tmp_chunk->offset,tmp_chunk->len);
						add_2_chunk_cache_head(cache_chunk);
					}
	
				}
				else
				{
					if((tmp_chunk->ab_offset == ab_offset)&&(!back_to_begin))
						GET_FPDD_DATA()->cache_hit++;
					if(chunk_used_again_in_window(tmp_chunk))
						move_2_chunk_cache_head(cache_chunk);
					else
						move_2_chunk_cache_after_s_prt(cache_chunk);
				}
				if((tmp_chunk->ab_offset == ab_offset)&&(!back_to_begin))
				{
					/*the chunk belongs to this assembly buffer*/
					memcpy(a_buf+a_buf_size, cache_chunk,tmp_chunk->len);
					a_buf_size = a_buf_size+tmp_chunk->len;
					GET_FPDD_DATA()->total_restore_size +=tmp_chunk->len;

					tmp_con->chunk_next = tmp_chunk->next;
					remove_chunk_info_hash(tmp_chunk);
					free(tmp_chunk);
					tmp_chunk = tmp_con->chunk_next;
					count++;
					if(count>=ASSEMBLE_BUF_SIZE)
					{
						count = 0;
						back_to_begin = true;
						break;
					}
				}
				else
				{
					tmp_chunk = tmp_chunk->next;
				}
			}
			/*the container will be used in the future AB*/
			if(tmp_con->chunk_next!=NULL)
			{
				pre_con = tmp_con;
				tmp_con = tmp_con->container_next;
			}			
			else
			{
				pre_con->container_next = tmp_con->container_next;
				free(tmp_con);
				tmp_con = pre_con->container_next;
			}
		}
		
	#ifdef CONTENT_FREE_RESTORE
		fseek(fp, 0, SEEK_SET);
		fwrite(a_buf, 1, a_buf_size, fp);
	#else	
		fwrite(a_buf, 1, a_buf_size, fp);
	#endif
		ab_offset++;
		//result_log("write to the file: %ld\n",a_buf_size);
		a_buf_size_t = a_buf_size_t+a_buf_size;
	}
	free(buf);
	free(container_buf);
	return a_buf_size_t;
}


uint32_t assemble_buf_batch_adaptive(char *a_buf, struct metadata * mtdata, struct dedup_manager * dedup, uint64_t restored_num, FILE *fp, int look_ahead_times, int *current_pcache)
{
	struct _container_info *table_head;
	table_head = (struct _container_info *)malloc(sizeof(struct _container_info));
	table_head->container_next = NULL;
	table_head->chunk_next = NULL;
	table_head->container_name = 0;
	uint32_t a_buf_size_r, a_buf_size;
	
	a_buf_size = construct_restore_table(mtdata, restored_num, table_head);

	a_buf_size_r = restore_from_table_chunk_cache_batch(restored_num, table_head, a_buf, fp, look_ahead_times, current_pcache);

	if(a_buf_size == a_buf_size_r)
	{
		free(table_head);
		return a_buf_size_r;
	}
	else
	{
		if(a_buf_size > a_buf_size_r)
			return a_buf_size;
		else
			return a_buf_size_r;
	}	
}




		
	
			
			
				
		











 







