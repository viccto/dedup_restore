/* 
 * adaptive_faa.c, it is used to do the regular rolling faa, and adaptive faa, lucc
 * author: zhichao cao
 * created date: 03/18/2017
 */

#include <stdio.h>
#include <ulockmgr.h>
#include <pthread.h>
#include <limits.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <time.h>
#include <sys/stat.h>
#include <fcntl.h>

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
#include "chunk_cache.h"
#include "look_ahead.h"
#include "adaptive_faa.h"
#include "optsmr.h"

struct _faa *faa_init(void)
{
	struct _faa *faa;
	faa = (struct _faa *)malloc(sizeof(struct _faa));
	faa->num = 0;
	faa->head_count = 0;
	faa->tail_count = 0;

	faa->buf_head.assemble_count = 99999999;
	faa->buf_head.filled_num = 0;
	faa->buf_head.buf = NULL;
	faa->buf_head.next = &(faa->buf_head);
	faa->buf_head.pre = &(faa->buf_head);
	return faa;
}


void faa_destroy(struct _faa *faa)
{
	struct _faa_buf *tmp;
	tmp = faa->buf_head.next;
	while(tmp!=&(faa->buf_head))
	{
		free(tmp->buf);
		faa_buf_remove(tmp);
		free(tmp);
		tmp = faa->buf_head.next;
	}
	return;
}


struct _faa_buf *faa_buf_init(uint64_t assemble_count)
{
	struct _faa_buf *faa_buf;
	faa_buf = (struct _faa_buf *)malloc(sizeof(struct _faa_buf));
	faa_buf->assemble_count = assemble_count;
	faa_buf->filled_num = 0;
	faa_buf->buf = (char *)malloc(MAX_CHUNK_LEN*ASSEMBLE_BUF_SIZE);
	faa_buf->next = NULL;
	faa_buf->pre = NULL;
	return faa_buf;
}

void faa_buf_destroy(struct _faa_buf *tmp)
{
	if(tmp==NULL)
		return;
	free(tmp->buf);
	free(tmp);
	return;
}

void adaptive_faa_parm_init(struct _adaptive_faa_parm *faa_parm)
{
	if(faa_parm==NULL)
		faa_parm=(struct _adaptive_faa_parm *)malloc(sizeof(struct _adaptive_faa_parm));
	
	faa_parm->container_reads=0;
	faa_parm->hit=0;
	faa_parm->miss=0;
	faa_parm->e_add=0;
	faa_parm->p_add=0;
	faa_parm->e2p=0;
	faa_parm->p2e=0;
	faa_parm->e_num=GET_FPDD_DATA()->chunk_cache->e_num;
	faa_parm->p_num=GET_FPDD_DATA()->chunk_cache->p_num;
	return;
}

void faa_buf_remove(struct _faa_buf *tmp)
{
	if(tmp==NULL)
		return;
	tmp->pre->next = tmp->next;
	tmp->next->pre = tmp->pre;
	return;
}

void faa_add_tail(struct _faa_buf *tmp)
{
	if(tmp==NULL)
		return;
	tmp->pre = GET_FPDD_DATA()->faa->buf_head.pre;
	tmp->next = GET_FPDD_DATA()->faa->buf_head.pre->next;
	GET_FPDD_DATA()->faa->buf_head.pre->next = tmp;
	GET_FPDD_DATA()->faa->buf_head.pre = tmp;

	GET_FPDD_DATA()->faa->num++;
	GET_FPDD_DATA()->faa->tail_count = tmp->assemble_count;
	//look_ahead_log("the current count: %lld, the tail_count:%lld\n",tmp->assemble_count, GET_FPDD_DATA()->faa->tail_count);

	return;
}

struct _faa_buf * faa_remove_head(void)
{
	struct _faa_buf *tmp;
	if(GET_FPDD_DATA()->faa->buf_head.next==&(GET_FPDD_DATA()->faa->buf_head))
		return NULL;
	
	tmp = GET_FPDD_DATA()->faa->buf_head.next;
	faa_buf_remove(tmp);
	
	GET_FPDD_DATA()->faa->num--;
	GET_FPDD_DATA()->faa->head_count = tmp->next->assemble_count;
	return tmp;
}

void adaptive_faa_adjust(struct _adaptive_faa_parm *faa_parm)
{
	int faa_increase_num, mem_num, faa_low, faa_high, faa_change, law_low, law_high;
	//faa_increase_num = (faa_parm->last_faa+faa_parm->last_cache)/20;
	faa_increase_num = 2;
	mem_num = faa_parm->last_faa+faa_parm->last_cache;
	//faa_low = (faa_parm->last_faa+faa_parm->last_cache)/8;
	faa_low = 2;
	faa_high = 5*(faa_parm->last_faa+faa_parm->last_cache)/8;
	law_high = (faa_parm->last_faa+faa_parm->last_cache)*20;
	law_low = (faa_parm->last_faa+faa_parm->last_cache)*2;

	if(faa_parm->container_reads>2)
	{
		/*more miss and less effective faa*/
		faa_parm->effective_faa=0;
		faa_parm->effective_cache++;
	}
	else
	{
		faa_parm->effective_faa++;
		faa_parm->effective_cache=0;
	}

	/*faa is good for the current case, due to low container reads, increase faa*/
	if(faa_parm->effective_faa>((int)(6*faa_parm->last_faa)))
	{
		if((faa_parm->last_faa+faa_increase_num-1)<=faa_high)
			faa_parm->faa_change = faa_increase_num;
		/*here might need more information, like p cache to decide weather we should decrease or not*/
		if(faa_parm->last_law-1<=law_low)
			faa_parm->law_change = 1;
		else
			faa_parm->law_change = 0;

		faa_parm->effective_faa=0;
	}
	else if(faa_parm->effective_cache>0)
	{
		if((faa_parm->last_faa-1)>=faa_low)
			faa_parm->faa_change=0;
		faa_parm->effective_cache=0;
		/*
		if(faa_parm->last_law+faa_parm->last_law/(faa_parm->last_cache)<=law_high)
		{
			if((faa_parm->p_num>(faa_parm->last_cache/2))&&(faa_parm->e_add>128))
				
				faa_parm->law_change = (int)(0.8*faa_parm->last_law/(faa_parm->last_cache));
			else
				faa_parm->law_change = (int)(0.5*faa_parm->last_law/(faa_parm->last_cache)); 
		}
		*/
		int s = law_high/70;
		int law_try = s-faa_parm->last_law/70;
		if(faa_parm->last_law+law_try<=law_high)
		{
			faa_parm->law_change = law_try;
		}
		else
			faa_parm->law_change = law_high-faa_parm->last_law;
	}
	else if(faa_parm->effective_faa%4==0)
	{
		faa_parm->faa_change = 1;
		if(faa_parm->last_law-1<=law_low)
			faa_parm->law_change = 1;
		else
			faa_parm->law_change = 0;
	}
	else
	{
		faa_parm->faa_change = 1;
		faa_parm->law_change = 1;
	}

	if((faa_parm->last_faa-1+faa_parm->faa_change<faa_low)||(faa_parm->last_faa-1+faa_parm->faa_change>faa_high))
	{
		faa_parm->faa_change = 1;
		faa_parm->law_change = 1;
	}

	if(faa_parm->e_add>512)
	{
		if(faa_parm->last_faa-1>=faa_low)
		{	
			faa_parm->faa_change = 0;
			int s = law_high/70;
			int law_try = s-faa_parm->last_law/70;
			if(faa_parm->last_law+law_try<=law_high)
				faa_parm->law_change = law_try;
			else
				faa_parm->law_change = law_high-faa_parm->last_law;
		}
		else
		{
			faa_parm->faa_change = 1;
			faa_parm->law_change = 0;
		}
	}

	if(faa_parm->p_num<512)
	{
		faa_parm->law_change = 0;
	}

}


int restore_assemble_adaptive_faa(uint64_t assemble_count, FILE *fp, struct _adaptive_faa_parm *faa_parm)
{
	struct _container_info *table_head;
	table_head = GET_FPDD_DATA()->look_ahead_table->table_head;
	struct _container_info *tmp_con, *pre_con;
	struct chunk_info * tmp_chunk, *chunk_tmp;
	struct _chunk_cache_node *cache_chunk, *tmp_cache_chunk;
	struct _faa *faa;
	struct _faa_buf *tmp_faa_buf;
	faa = GET_FPDD_DATA()->faa;
	char *container_buf;
	char *buf;
	uint32_t a_buf_size;
	int count, ab_offset, i;
	bool not_cached, get_container;

	buf = malloc(MAX_CHUNK_LEN);
	container_buf = malloc(MAX_CONTAINER_DATA);
	a_buf_size = 0;
	count = 0;
	adaptive_faa_parm_init(faa_parm);
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

		while(tmp_chunk!=NULL)
		{
			/*if chunk assembly_buffer offset is larger than tail_count, it will not be used in current faa, move to next*/
			if(tmp_chunk->ab_offset != assemble_count)
				break;

				/*check the cache first*/
			cache_chunk = find_chunk_cache(tmp_chunk->chunk_name);
			if(cache_chunk==NULL)
			{
				faa_parm->miss++;
					/*chunk not in the cache, also not in the
					 * container_buf need to read from disk*/
						/*read in the container*/
				GET_FPDD_DATA()->cache_miss++;
				GET_FPDD_DATA()->total_container_read++;
				read_container_to_buf(container_buf, tmp_con->container_name);
				faa_parm->container_reads++;				
				tmp_faa_buf = faa->buf_head.next;

				/*deal with the rest of chunks in the same container*/
				while(tmp_chunk!=NULL)
				{
					/*the chunk is in the faa, pre-alocate it, do not use cache, use the container i/O buffer directly*/
					if(tmp_chunk->ab_offset<=faa->tail_count)
					{
						
						while(tmp_faa_buf->assemble_count<tmp_chunk->ab_offset)
							tmp_faa_buf = tmp_faa_buf->next;
						//look_ahead_log("ad_offset: %lld, faa->tail_count: %lld, faa_buf->assembly_count: %lld\n",tmp_chunk->ab_offset, faa->tail_count, tmp_faa_buf->assemble_count);
						memcpy(tmp_faa_buf->buf+tmp_chunk->a_buf_pos, container_buf+tmp_chunk->offset,tmp_chunk->len);
						a_buf_size = a_buf_size+tmp_chunk->len;
						GET_FPDD_DATA()->total_restore_size +=tmp_chunk->len;
						tmp_faa_buf->filled_num++;

						/*even the chunk is used in FAA, still cache it if P-cache space is still avaiable*/
						if(chunk_cache_pcache_size()>0)
						{
							cache_chunk = find_chunk_cache(tmp_chunk->chunk_name);
							if(cache_chunk==NULL)
							{
								cache_chunk = (struct _chunk_cache_node *)malloc(sizeof(struct _chunk_cache_node));
								cache_chunk->chunk_ptr = malloc(MAX_CHUNK_LEN);
								cache_chunk->chunk_name = tmp_chunk->chunk_name;
								memcpy(cache_chunk->chunk_ptr, container_buf+tmp_chunk->offset,tmp_chunk->len);
								add_2_chunk_cache_after_s_prt(cache_chunk);
								faa_parm->p_add++;
								
							}
						}


						tmp_con->chunk_next = tmp_chunk->next;
						remove_chunk_info_hash(tmp_chunk);
						free(tmp_chunk);
						tmp_chunk = tmp_con->chunk_next;


					}
					else
					/*the chunk is out of faa but in LAW, so if used, put in the cache*/
					{
						cache_chunk = find_chunk_cache(tmp_chunk->chunk_name);
						if(cache_chunk==NULL)
						{
							cache_chunk = (struct _chunk_cache_node *)malloc(sizeof(struct _chunk_cache_node));
							cache_chunk->chunk_ptr = malloc(MAX_CHUNK_LEN);
							cache_chunk->chunk_name = tmp_chunk->chunk_name;
							memcpy(cache_chunk->chunk_ptr, container_buf+tmp_chunk->offset,tmp_chunk->len);
							add_2_chunk_cache_before_s_prt(cache_chunk);
							faa_parm->e_add++;
								
						}
						else
						{
							if(cache_chunk->chunk_state==0)
							{
								move_2_chunk_cache_before_s_prt(cache_chunk);
								faa_parm->p2e++;
								faa_parm->e_add++;
							}
						}
						tmp_chunk = tmp_chunk->next;
					}
	
				}
			}
			else
			/*chunk is in the cache, use it and move it to the head it reused*/
			{
				/*put in the faa*/
				faa_parm->hit++;
				GET_FPDD_DATA()->cache_hit++;
				memcpy(faa->buf_head.next->buf+tmp_chunk->a_buf_pos, cache_chunk->chunk_ptr,tmp_chunk->len);
				a_buf_size = a_buf_size+tmp_chunk->len;
				GET_FPDD_DATA()->total_restore_size +=tmp_chunk->len;
				faa->buf_head.next->filled_num++;

				/*adjust the cache*/
				if(chunk_used_again_in_window(tmp_chunk))
				{
					if(cache_chunk->chunk_state==0)
					{
						move_2_chunk_cache_head(cache_chunk);
						faa_parm->p2e++;
						faa_parm->e_add++;
					}
				}
				else
				{
					if(cache_chunk->chunk_state==1)
					{
						move_2_chunk_cache_after_s_prt(cache_chunk);
						faa_parm->e2p++;
					}

					faa_parm->p_add++;
				}
				/*remove from the LAW*/
				tmp_con->chunk_next = tmp_chunk->next;
				remove_chunk_info_hash(tmp_chunk);
				free(tmp_chunk);
				tmp_chunk = tmp_con->chunk_next;

				
				
			}
	
		}

		/*the container will be used in the future*/
		if(tmp_con->chunk_next!=NULL)
		{
			pre_con = tmp_con;
			tmp_con = tmp_con->container_next;
		}			
		else
		/*the container is totally used, remove it*/
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
	fwrite(faa->buf_head.next->buf, 1, a_buf_size, fp);
#endif
	look_ahead_log("write to the faa chunk number: %d\n",faa->buf_head.next->filled_num);
	
	//result_log("%d; %d %d; %d %d; %d %d; %d %d %d; %d %d; %d %d;", faa_parm->container_reads, faa_parm->hit, faa_parm->miss, faa_parm->effective_faa, faa_parm->effective_cache, faa_parm->faa_change, faa_parm->law_change, faa_parm->last_faa, faa_parm->last_cache, faa_parm->last_law, faa_parm->e_add, faa_parm->p_add, faa_parm->e2p, faa_parm->p2e);
	result_log("%d %d %d %d %d %d %d %d %d %d ", faa_parm->container_reads, faa_parm->hit, faa_parm->miss, faa_parm->last_faa, faa_parm->last_cache, faa_parm->last_law,faa_parm->e_add, faa_parm->p_add, faa_parm->e2p, faa_parm->p2e);
	result_log("%lld %lld %lld\n", GET_FPDD_DATA()->chunk_cache->cached_num, GET_FPDD_DATA()->chunk_cache->e_num, GET_FPDD_DATA()->chunk_cache->p_num);

	free(container_buf);
	return a_buf_size;
}



























