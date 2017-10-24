/* 
 * trace_work.h, it is used to handle the dedup trace
 * authoer: zhichao cao
 * created date: 02/10/2017
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
#include "trace_work.h"
#include "optsmr.h"

#define F_SIZE 16
int trace_read_line(char *buf, FILE *file)
{
	if(fgets(buf, 100, file)==NULL)
		return -1;
	else
		return 1;
}


/* process the trace line read in
 * change to the fingerprint and size
 */
int process_trace_line(char *line, struct metadata* mtdata)
{
	int i,j,order;
	char data[5];
	memcpy(mtdata->fingerprint, line, 17);
	int num=0;
	for(i=19;i<25;i++)
	{
		if(line[i]==' ')
			break;
		data[num] = line[i];
		num++;
	}
	order = 1;
	mtdata->len = 0;
	for(i=0;i<num;i++)
	{
		mtdata->len = mtdata->len+order*((int)(data[num-i-1]-48));
		order = order*10;
	}
	mtdata->len=4096;
	return 0;
}

int dedup_trace_file(int64_t chunk_num, char * file_name)
{
	FILE *trace_file, *recipe_file, *source_file;
	char line[100], recipe[PATH_MAX], trace[PATH_MAX];
	char *buf;
	int i;
	struct metadata *mtdata;
	mtdata = (struct metadata*)malloc(sizeof(struct metadata));
	struct _recipe_meta *recipe_meta;
	recipe_meta = (struct _recipe_meta *)malloc(sizeof(struct _recipe_meta));
	printf("recipe");
	strcpy(recipe, GET_FPDD_DATA()->recipedir);
	strcat(recipe, file_name);
	strcpy(trace, GET_FPDD_DATA()->tracedir);
	strcat(trace, file_name);
	printf("recipe: %s,,file: %s\n", recipe, trace);
	
	trace_file = fopen(trace,"r");
	recipe_file = fopen(recipe, "w");
	source_file = fopen("./source.txt", "r");
	if((trace_file==NULL)||(recipe_file==NULL)||(source_file==NULL)){
		printf("open file NULL\n");
		return -1;
	}

	recipe_meta->chunk_num = chunk_num;
	strcpy(recipe_meta->trace_name, file_name);
	fwrite(recipe_meta, 1, sizeof(struct _recipe_meta), recipe_file);

	
	trace_read_line(line, trace_file);
	for(i=0;i<chunk_num;i++){
		memset(mtdata->fingerprint, 'a', FINGERPRINT_LEN);
		trace_read_line(line, trace_file);
		process_trace_line(line, mtdata);
		buf = malloc(mtdata->len);
		memset(buf, '-', mtdata->len);
		//fread(buf, 1, mtdata->len, source_file);
		index_dedup(buf, mtdata, GET_FPDD_DATA()->dedup, recipe_file);
		free(buf);
	}
	result_log("container files written to disk: %d\n", GET_FPDD_DATA()->container_counter);
	free(mtdata);
	free(recipe_meta);
	fclose(trace_file);
	fclose(recipe_file);
	fclose(source_file);
	return 0;
}



/*this function is used to reload the file back to 
 * orginal state and using the look ahead window*/
int trace_restore_look_ahead(uint64_t chunk_num, char * file_name)
{
	container_log("enter write_file............. \n");
	FILE * fp, * metafp;
	char * a_buf;
	uint32_t a_size;
	uint64_t restored_num, dedup_num;
	int ret;
	int i;
	char metapath[PATH_MAX], restorepath[PATH_MAX];
	struct metadata * mtdata;
	struct disk_hash_node node;
	
	/*process the metadata and restore file path*/
	strcpy(metapath,GET_FPDD_DATA()->recipedir);
	strcat(metapath,file_name);
	strcpy(restorepath,GET_FPDD_DATA()->restoredir);
	strcat(restorepath,file_name);

	struct _recipe_meta *recipe_meta;
	recipe_meta = (struct _recipe_meta *)malloc(sizeof(struct _recipe_meta));
	
	/*read out the recipe_meta and file recipe from head*/
	metafp=fopen(metapath,"r");
	if (metafp==NULL) 
	{
		error_log("Open file error:%s\n",metapath);
	}
	fseek(metafp, 0, SEEK_SET);
	fread(recipe_meta,sizeof(struct _recipe_meta),1,metafp);      //get the number of chunks
	recipe_meta->chunk_num = chunk_num;
	restored_num = 0;
	dedup_num = recipe_meta->chunk_num;
	if(recipe_meta->chunk_num>=LOOK_AHEAD_SIZE)
	{	
		mtdata=(struct metadata *)malloc(LOOK_AHEAD_SIZE*sizeof(struct metadata));
		fread(mtdata,sizeof(struct metadata),LOOK_AHEAD_SIZE, metafp);
		restored_num = LOOK_AHEAD_SIZE;
	}
	else
	{
		mtdata=(struct metadata *)malloc(recipe_meta->chunk_num*sizeof(struct metadata));
		fread(mtdata,sizeof(struct metadata),recipe_meta->chunk_num, metafp);
		restored_num = recipe_meta->chunk_num;
	}
	a_buf = malloc(MAX_CHUNK_LEN*LOOK_AHEAD_SIZE);
	fp = fopen(restorepath, "w+");

	while(dedup_num>0)
	{
		
		/*the function of look ahead to full fill the a_buf*/
		a_size = assemble_buf(a_buf, mtdata, GET_FPDD_DATA()->dedup, restored_num, fp);
		/*when this look ahead window is full, write to the file and start a new one*/
		//fwrite(a_buf, 1, a_size, fp);
		free(mtdata);

		dedup_num = dedup_num - restored_num;
		if(dedup_num>=LOOK_AHEAD_SIZE)
		{	
			mtdata=(struct metadata *)malloc(LOOK_AHEAD_SIZE*sizeof(struct metadata));
			fread(mtdata,sizeof(struct metadata),LOOK_AHEAD_SIZE, metafp);
			restored_num = LOOK_AHEAD_SIZE;
		}
		else
		{
			if(dedup_num>0){
				mtdata=(struct metadata *)malloc(dedup_num*sizeof(struct metadata));
				fread(mtdata,sizeof(struct metadata),dedup_num, metafp);
				restored_num = dedup_num;
			}
		}
		
	}

	fclose(fp);
	fclose(metafp);
	free(a_buf);
	free(recipe_meta);

	return 0;
}


/*another adaptive restore, it read several assemble buff
 * then restore, each time, the segment size is variable*/
int trace_restore_look_ahead_batch_adaptive(char * file_name, int initial_look_num)
{
	container_log("enter write_file............. \n");
	FILE * fp, * metafp;
	char * a_buf;
	uint32_t a_size;
	uint64_t restored_num, dedup_num,  last_look_ahead_num, current_look_ahead_num;
	int current_pcache;
	int ret;
	int i;
	char metapath[PATH_MAX], restorepath[PATH_MAX];
	struct metadata * mtdata;
	struct disk_hash_node node;
	
	/*process the metadata and restore file path*/
	strcpy(metapath,GET_FPDD_DATA()->recipedir);
	strcat(metapath,file_name);
	strcpy(restorepath,GET_FPDD_DATA()->restoredir);
	strcat(restorepath,file_name);

	struct _recipe_meta *recipe_meta;
	recipe_meta = (struct _recipe_meta *)malloc(sizeof(struct _recipe_meta));
	
	/*read out the recipe_meta and file recipe from head*/
	metafp=fopen(metapath,"r");
	if (metafp==NULL) 
	{
		error_log("Open file error:%s\n",metapath);
	}
	fseek(metafp, 0, SEEK_SET);
	fread(recipe_meta,sizeof(struct _recipe_meta),1,metafp);      //get the number of chunks
	
	restored_num = 0;
	dedup_num = recipe_meta->chunk_num;
	if(recipe_meta->chunk_num>=(initial_look_num*ASSEMBLE_BUF_SIZE))
	{	
		mtdata=(struct metadata *)malloc((initial_look_num*ASSEMBLE_BUF_SIZE)*sizeof(struct metadata));
		fread(mtdata,sizeof(struct metadata),(initial_look_num*ASSEMBLE_BUF_SIZE), metafp);
		restored_num = (initial_look_num*ASSEMBLE_BUF_SIZE);
	}
	else
	{
		mtdata=(struct metadata *)malloc(recipe_meta->chunk_num*sizeof(struct metadata));
		fread(mtdata,sizeof(struct metadata),recipe_meta->chunk_num, metafp);
		restored_num = recipe_meta->chunk_num;
	}
	a_buf = malloc(MAX_CHUNK_LEN*(AHEAD_NUM_HIGH_LIMIT*ASSEMBLE_BUF_SIZE));
	fp = fopen(restorepath, "w+");
	last_look_ahead_num = initial_look_num;
	
	
	while(dedup_num>0)
	{
		
		/*the function of look ahead to full fill the a_buf*/
		a_size = assemble_buf_batch_adaptive(a_buf, mtdata, GET_FPDD_DATA()->dedup, restored_num, fp, last_look_ahead_num, &current_pcache);
		free(mtdata);
		dedup_num = dedup_num - restored_num;

		if(current_pcache>5)
		{
			current_look_ahead_num = 1+last_look_ahead_num+current_pcache/((GET_FPDD_DATA()->chunk_cache->CHUNK_CACHE_NUM-current_pcache)/last_look_ahead_num);
			if(current_look_ahead_num>AHEAD_NUM_HIGH_LIMIT)
				current_look_ahead_num = AHEAD_NUM_HIGH_LIMIT;
		}
		else
		{
			if(last_look_ahead_num>AHEAD_NUM_LOW_LIMIT)
				current_look_ahead_num = last_look_ahead_num-1;
			else
				current_look_ahead_num = last_look_ahead_num;
		}
				
		look_ahead_log("next window size: %lld, pcache size: %d, current_pcache: %d, pre-restore size:%ld\n",current_look_ahead_num, chunk_cache_pcache_size(), current_pcache,a_size);
		if(dedup_num>=(current_look_ahead_num*ASSEMBLE_BUF_SIZE))
		{	
			mtdata=(struct metadata *)malloc((current_look_ahead_num*ASSEMBLE_BUF_SIZE)*sizeof(struct metadata));
			fread(mtdata,sizeof(struct metadata),(current_look_ahead_num*ASSEMBLE_BUF_SIZE), metafp);
			restored_num = (current_look_ahead_num*ASSEMBLE_BUF_SIZE);
			last_look_ahead_num = current_look_ahead_num;
		}
		else
		{
			if(dedup_num>0){
				mtdata=(struct metadata *)malloc(dedup_num*sizeof(struct metadata));
				fread(mtdata,sizeof(struct metadata),dedup_num, metafp);
				restored_num = dedup_num;
			}
		}
		
	}

	fclose(fp);
	fclose(metafp);
	free(a_buf);
	free(recipe_meta);

	return 0;
}


/*
 * the adaptive look ahead with container 
 * and chunk caching, the caching size could 
 * be configured*/

int restore_pipe_line_adaptive_look_ahead(uint64_t chunk_num, char * file_name, int initial_look_num)
{
	container_log("enter write_file............. \n");
	FILE * fp, * metafp;
	char * a_buf;
	uint32_t a_size;
	uint64_t restored_num, dedup_num, look_ahead_num, wait_num;
	uint64_t assemble_count, look_ahead_count;
	int ret;
	int i, current_pcache, average_ecache_in_law;
	char metapath[PATH_MAX], restorepath[PATH_MAX];
	struct metadata * mtdata;
	struct disk_hash_node node;
	
	/*process the metadata and restore file path*/
	strcpy(metapath,GET_FPDD_DATA()->recipedir);
	strcat(metapath,file_name);
	strcpy(restorepath,GET_FPDD_DATA()->restoredir);
	strcat(restorepath,file_name);

	struct _recipe_meta *recipe_meta;
	recipe_meta = (struct _recipe_meta *)malloc(sizeof(struct _recipe_meta));
	
	/*read out the recipe_meta and file recipe from head*/
	metafp=fopen(metapath,"r");
	if (metafp==NULL) 
	{
		error_log("Open file error:%s\n",metapath);
	}
	fseek(metafp, 0, SEEK_SET);
	fread(recipe_meta,sizeof(struct _recipe_meta),1,metafp);      //get the number of chunks
	recipe_meta->chunk_num = chunk_num;
	restored_num = 0;
	look_ahead_num = 0;
	look_ahead_count = 0;
	wait_num = recipe_meta->chunk_num;
	dedup_num = recipe_meta->chunk_num;
	
	/*add the inital recipe to the look ahead window*/
	if(recipe_meta->chunk_num>=(ASSEMBLE_BUF_SIZE*initial_look_num))
	{	
		for(i=0;i<initial_look_num;i++)
		{
			mtdata=(struct metadata *)malloc(ASSEMBLE_BUF_SIZE*sizeof(struct metadata));
			fread(mtdata,sizeof(struct metadata),ASSEMBLE_BUF_SIZE, metafp);
			add_to_look_ahead_window(mtdata, ASSEMBLE_BUF_SIZE, look_ahead_count);
			look_ahead_count++;
			look_ahead_num = look_ahead_num + ASSEMBLE_BUF_SIZE;
			GET_FPDD_DATA()->current_look_ahead_size++;
			free(mtdata);
		}
	}
	else
	{
		while(wait_num>0)
		{
			if(wait_num>=ASSEMBLE_BUF_SIZE)
			{
				mtdata=(struct metadata *)malloc(ASSEMBLE_BUF_SIZE*sizeof(struct metadata));
				fread(mtdata,sizeof(struct metadata),ASSEMBLE_BUF_SIZE, metafp);
				add_to_look_ahead_window(mtdata, ASSEMBLE_BUF_SIZE, look_ahead_count);
				look_ahead_count++;
				look_ahead_num = look_ahead_num + ASSEMBLE_BUF_SIZE;
				wait_num = wait_num - ASSEMBLE_BUF_SIZE;
				free(mtdata);
			}
			else
			{
				mtdata=(struct metadata *)malloc(wait_num*sizeof(struct metadata));
				fread(mtdata,sizeof(struct metadata),wait_num, metafp);
				add_to_look_ahead_window(mtdata, wait_num, look_ahead_count);
				look_ahead_count++;
				look_ahead_num = look_ahead_num + wait_num;
				wait_num = 0;
				free(mtdata);
			}
			GET_FPDD_DATA()->current_look_ahead_size++;
		}
		
	}

	a_buf = malloc(MAX_CHUNK_LEN*ASSEMBLE_BUF_SIZE);
	fp = fopen(restorepath, "w+");
	bool need_look_ahead;

	assemble_count = 0;

	look_ahead_log("window size: %lld, pcache size: %d\n",GET_FPDD_DATA()->current_look_ahead_size, chunk_cache_pcache_size());
	while(dedup_num>0)
	{
		if(dedup_num>=ASSEMBLE_BUF_SIZE)
		{	
			/*assemble the current one*/
			a_size = restore_assemble_buf_write(a_buf, ASSEMBLE_BUF_SIZE, assemble_count, fp);
			GET_FPDD_DATA()->current_look_ahead_size--;
			assemble_count++;
			dedup_num = dedup_num - ASSEMBLE_BUF_SIZE;
			current_pcache = chunk_cache_pcache_size();
			average_ecache_in_law = (GET_FPDD_DATA()->chunk_cache->CHUNK_CACHE_NUM-current_pcache)/GET_FPDD_DATA()->current_look_ahead_size;

			
			if((recipe_meta->chunk_num-look_ahead_num)>=ASSEMBLE_BUF_SIZE)
			{
				need_look_ahead = false;
				if((look_ahead_num-(recipe_meta->chunk_num-dedup_num))<=(AHEAD_NUM_LOW_LIMIT*ASSEMBLE_BUF_SIZE))
				{
					/*smaller than the look ahead required num, add it*/
					need_look_ahead = true;
				}
				else
				{
					if(can_do_look_ahead())
						need_look_ahead = true;
				}
				
				if(need_look_ahead)
				{
					while((current_pcache>=0)&&(GET_FPDD_DATA()->current_look_ahead_size<AHEAD_NUM_HIGH_LIMIT))
					{
						mtdata=(struct metadata *)malloc(ASSEMBLE_BUF_SIZE*sizeof(struct metadata));
						fread(mtdata,sizeof(struct metadata),ASSEMBLE_BUF_SIZE, metafp);
						add_to_look_ahead_window(mtdata, ASSEMBLE_BUF_SIZE, look_ahead_count);
						GET_FPDD_DATA()->current_look_ahead_size++;
						current_pcache-=average_ecache_in_law;
						look_ahead_count++;
						look_ahead_num = look_ahead_num + ASSEMBLE_BUF_SIZE;
						wait_num = wait_num - ASSEMBLE_BUF_SIZE;
						free(mtdata);
						look_ahead_log("window size: %lld, pcache size: %d\n",GET_FPDD_DATA()->current_look_ahead_size, chunk_cache_pcache_size());
					}
				}
			}
			else
			{
				/*if it is the last part less than a window size, add it*/
				wait_num = recipe_meta->chunk_num-look_ahead_num;
				mtdata=(struct metadata *)malloc(wait_num*sizeof(struct metadata));
				fread(mtdata,sizeof(struct metadata),wait_num, metafp);
				add_to_look_ahead_window(mtdata, wait_num, look_ahead_count);
				GET_FPDD_DATA()->current_look_ahead_size++;
				look_ahead_count++;
				look_ahead_num = look_ahead_num + wait_num;
				free(mtdata);
			}
					
		}
		else
		{
			if(dedup_num>0){
				a_size = restore_assemble_buf_write(a_buf, dedup_num,assemble_count,fp);
				GET_FPDD_DATA()->current_look_ahead_size--;
				assemble_count++;
				dedup_num = 0;
			}
		}
		
	}

	fclose(fp);
	fclose(metafp);
	free(a_buf);
	free(recipe_meta);

	return 0;
}



int restore_pipe_line_look_ahead(uint64_t chunk_num, char * file_name, int initial_look_num, int cache_mode)
{
	container_log("enter restore_pipe_line_look_ahead............. \n");
	FILE * fp, * metafp;
	char * a_buf;
	uint32_t a_size;
	uint64_t restored_num, dedup_num, look_ahead_num, wait_num;
	uint64_t assemble_count, look_ahead_count;
	int ret;
	int i, current_pcache, average_ecache_in_law;
	char metapath[PATH_MAX], restorepath[PATH_MAX];
	struct metadata * mtdata;
	struct disk_hash_node node;
	
	/*process the metadata and restore file path*/
	strcpy(metapath,GET_FPDD_DATA()->recipedir);
	strcat(metapath,file_name);
	strcpy(restorepath,GET_FPDD_DATA()->restoredir);
	strcat(restorepath,file_name);

	struct _recipe_meta *recipe_meta;
	recipe_meta = (struct _recipe_meta *)malloc(sizeof(struct _recipe_meta));
	
	/*read out the recipe_meta and file recipe from head*/
	metafp=fopen(metapath,"r");
	if (metafp==NULL) 
	{
		error_log("Open file error:%s\n",metapath);
	}
	fseek(metafp, 0, SEEK_SET);
	fread(recipe_meta,sizeof(struct _recipe_meta),1,metafp);      //get the number of chunks
	recipe_meta->chunk_num = chunk_num;
	restored_num = 0;
	look_ahead_num = 0;
	look_ahead_count = 0;
	wait_num = recipe_meta->chunk_num;
	dedup_num = recipe_meta->chunk_num;
	
	/*add the inital recipe to the look ahead window*/
	if(recipe_meta->chunk_num>=(ASSEMBLE_BUF_SIZE*initial_look_num))
	{	
		for(i=0;i<LOOK_AHEAD_TIMES;i++)
		{
			mtdata=(struct metadata *)malloc(ASSEMBLE_BUF_SIZE*sizeof(struct metadata));
			fread(mtdata,sizeof(struct metadata),ASSEMBLE_BUF_SIZE, metafp);
			add_to_look_ahead_window(mtdata, ASSEMBLE_BUF_SIZE, look_ahead_count);
			look_ahead_count++;
			look_ahead_num = look_ahead_num + ASSEMBLE_BUF_SIZE;
			GET_FPDD_DATA()->current_look_ahead_size++;
			free(mtdata);
		}
	}
	else
	{
		while(wait_num>0)
		{
			if(wait_num>=ASSEMBLE_BUF_SIZE)
			{
				mtdata=(struct metadata *)malloc(ASSEMBLE_BUF_SIZE*sizeof(struct metadata));
				fread(mtdata,sizeof(struct metadata),ASSEMBLE_BUF_SIZE, metafp);
				add_to_look_ahead_window(mtdata, ASSEMBLE_BUF_SIZE, look_ahead_count);
				look_ahead_count++;
				look_ahead_num = look_ahead_num + ASSEMBLE_BUF_SIZE;
				wait_num = wait_num - ASSEMBLE_BUF_SIZE;
				free(mtdata);
			}
			else
			{
				mtdata=(struct metadata *)malloc(wait_num*sizeof(struct metadata));
				fread(mtdata,sizeof(struct metadata),wait_num, metafp);
				add_to_look_ahead_window(mtdata, wait_num, look_ahead_count);
				look_ahead_count++;
				look_ahead_num = look_ahead_num + wait_num;
				wait_num = 0;
				free(mtdata);
			}
			GET_FPDD_DATA()->current_look_ahead_size++;
		}
		
	}

	a_buf = malloc(MAX_CHUNK_LEN*ASSEMBLE_BUF_SIZE);
	fp = fopen(restorepath, "w+");
	bool need_look_ahead;

	assemble_count = 0;

	look_ahead_log("window size: %lld, pcache size: %d\n",GET_FPDD_DATA()->current_look_ahead_size, chunk_cache_pcache_size());
	while(dedup_num>0)
	{
		if(dedup_num>=ASSEMBLE_BUF_SIZE)
		{	
			/*assemble the current one*/
			a_size = restore_assemble_pipe_line_buf_write(a_buf, ASSEMBLE_BUF_SIZE, assemble_count, fp, cache_mode);
			GET_FPDD_DATA()->current_look_ahead_size--;
			assemble_count++;
			dedup_num = dedup_num - ASSEMBLE_BUF_SIZE;
			current_pcache = chunk_cache_pcache_size();
			average_ecache_in_law = (GET_FPDD_DATA()->chunk_cache->CHUNK_CACHE_NUM-current_pcache)/GET_FPDD_DATA()->current_look_ahead_size;

			
			if((recipe_meta->chunk_num-look_ahead_num)>=ASSEMBLE_BUF_SIZE)
			{
				mtdata=(struct metadata *)malloc(ASSEMBLE_BUF_SIZE*sizeof(struct metadata));
				fread(mtdata,sizeof(struct metadata),ASSEMBLE_BUF_SIZE, metafp);
				add_to_look_ahead_window(mtdata, ASSEMBLE_BUF_SIZE, look_ahead_count);
				GET_FPDD_DATA()->current_look_ahead_size++;
				current_pcache-=average_ecache_in_law;
				look_ahead_count++;
				look_ahead_num = look_ahead_num + ASSEMBLE_BUF_SIZE;
				wait_num = wait_num - ASSEMBLE_BUF_SIZE;
				free(mtdata);
				look_ahead_log("window size: %lld, pcache size: %d\n",GET_FPDD_DATA()->current_look_ahead_size, chunk_cache_pcache_size());
			}
			else
			{
				/*if it is the last part less than a window size, add it*/
				wait_num = recipe_meta->chunk_num-look_ahead_num;
				mtdata=(struct metadata *)malloc(wait_num*sizeof(struct metadata));
				fread(mtdata,sizeof(struct metadata),wait_num, metafp);
				add_to_look_ahead_window(mtdata, wait_num, look_ahead_count);
				GET_FPDD_DATA()->current_look_ahead_size++;
				look_ahead_count++;
				look_ahead_num = look_ahead_num + wait_num;
				free(mtdata);
			}
					
		}
		else
		{
			if(dedup_num>0){
				a_size = restore_assemble_pipe_line_buf_write(a_buf, dedup_num,assemble_count,fp, cache_mode);
				GET_FPDD_DATA()->current_look_ahead_size--;
				assemble_count++;
				dedup_num = 0;
			}
		}
		
	}

	fclose(fp);
	fclose(metafp);
	free(a_buf);
	free(recipe_meta);

	return 0;
}

int restore_pipe_line_adaptive_faa(uint64_t chunk_num, char * file_name, int initial_look_num, int initial_faa_num, int initial_cache_num)
{
	container_log("restore_pipe_line_adaptive_faa............. \n");	
	GET_FPDD_DATA()->chunk_cache->CHUNK_CACHE_NUM = initial_cache_num*ASSEMBLE_BUF_SIZE;
	GET_FPDD_DATA()->chunk_cache->p_num = 1;

	FILE * fp, * metafp;
	uint32_t a_size;
	uint64_t restored_num, dedup_num, look_ahead_num, wait_num;
	uint64_t assemble_count, look_ahead_count, next_faa_count, p_cache_size, stat_count;
	int ret;
	int i,j, current_pcache, average_ecache_in_law, cache_change;
	char metapath[PATH_MAX], restorepath[PATH_MAX];
	struct metadata * mtdata;
	struct disk_hash_node node;
	struct _faa_buf *tmp_faa_buf;

	struct _adaptive_faa_parm faa_parm;
	adaptive_faa_parm_init(&faa_parm);
	faa_parm.last_faa = initial_faa_num;
	faa_parm.last_cache = initial_cache_num;
	faa_parm.last_law = initial_look_num;
	faa_parm.effective_faa = 0;
	faa_parm.effective_cache = 0;
	
	
	/*process the metadata and restore file path*/
	strcpy(metapath,GET_FPDD_DATA()->recipedir);
	strcat(metapath,file_name);
	strcpy(restorepath,GET_FPDD_DATA()->restoredir);
	strcat(restorepath,file_name);

	struct _recipe_meta *recipe_meta;
	recipe_meta = (struct _recipe_meta *)malloc(sizeof(struct _recipe_meta));
	
	/*read out the recipe_meta and file recipe from head*/
	metafp=fopen(metapath,"r");
	if (metafp==NULL) 
	{
		error_log("Open file error:%s\n",metapath);
	}
	fseek(metafp, 0, SEEK_SET);
	fread(recipe_meta,sizeof(struct _recipe_meta),1,metafp);      //get the number of chunks
	recipe_meta->chunk_num = chunk_num;
	restored_num = 0;
	look_ahead_num = 0;
	look_ahead_count = 0;
	wait_num = recipe_meta->chunk_num;
	dedup_num = recipe_meta->chunk_num;
	
	/*add the inital recipe to the look ahead window*/
	if(recipe_meta->chunk_num>=(ASSEMBLE_BUF_SIZE*initial_look_num))
	{	
		for(i=0;i<initial_look_num;i++)
		{
			mtdata=(struct metadata *)malloc(ASSEMBLE_BUF_SIZE*sizeof(struct metadata));
			fread(mtdata,sizeof(struct metadata),ASSEMBLE_BUF_SIZE, metafp);
			add_to_look_ahead_window(mtdata, ASSEMBLE_BUF_SIZE, look_ahead_count);
			look_ahead_count++;
			look_ahead_num = look_ahead_num + ASSEMBLE_BUF_SIZE;
			GET_FPDD_DATA()->current_look_ahead_size++;
			free(mtdata);
			
	
		}
	}
	else
	{
		while(wait_num>0)
		{
			if(wait_num>=ASSEMBLE_BUF_SIZE)
			{
				mtdata=(struct metadata *)malloc(ASSEMBLE_BUF_SIZE*sizeof(struct metadata));
				fread(mtdata,sizeof(struct metadata),ASSEMBLE_BUF_SIZE, metafp);
				add_to_look_ahead_window(mtdata, ASSEMBLE_BUF_SIZE, look_ahead_count);
				look_ahead_count++;
				look_ahead_num = look_ahead_num + ASSEMBLE_BUF_SIZE;
				wait_num = wait_num - ASSEMBLE_BUF_SIZE;
				free(mtdata);
			}
			else
			{
				mtdata=(struct metadata *)malloc(wait_num*sizeof(struct metadata));
				fread(mtdata,sizeof(struct metadata),wait_num, metafp);
				add_to_look_ahead_window(mtdata, wait_num, look_ahead_count);
				look_ahead_count++;
				look_ahead_num = look_ahead_num + wait_num;
				wait_num = 0;
				free(mtdata);
			}
			GET_FPDD_DATA()->current_look_ahead_size++;
		}
		
	}

	next_faa_count = 0;
	for(i=0;i<initial_faa_num;i++)
	{
		/*add a new faa*/
		tmp_faa_buf = faa_buf_init(next_faa_count);
		faa_add_tail(tmp_faa_buf);
		next_faa_count++;
	}

	fp = fopen(restorepath, "w+");
	bool need_look_ahead;

	assemble_count = 0;

	look_ahead_log("window size: %lld, pcache size: %d, tail_count: %lld\n",GET_FPDD_DATA()->current_look_ahead_size, chunk_cache_pcache_size(), GET_FPDD_DATA()->faa->tail_count);


	p_cache_size = 0; 
	stat_count = 0;
	while(dedup_num>0)
	{
		p_cache_size+=chunk_cache_pcache_size();
		stat_count++;
		//result_log("p_cache size: %lu, e_chunk size:%lu\n", chunk_cache_pcache_size(),GET_FPDD_DATA()->chunk_cache->e_num);
		if(dedup_num>=ASSEMBLE_BUF_SIZE)
		{	
			/*assemble the current one*/
			a_size = restore_assemble_adaptive_faa(assemble_count, fp, &faa_parm);
			GET_FPDD_DATA()->current_look_ahead_size--;
			assemble_count++;
			dedup_num = dedup_num - ASSEMBLE_BUF_SIZE;
			
			/*process and delete the current faa, add a new faa*/
			tmp_faa_buf = faa_remove_head();
			faa_buf_destroy(tmp_faa_buf);
			adaptive_faa_adjust(&faa_parm);
			for(j=0;j<faa_parm.faa_change;j++)
			{
				tmp_faa_buf = faa_buf_init(next_faa_count);
				faa_add_tail(tmp_faa_buf);
				next_faa_count++;
			}
			cache_change = 1-faa_parm.faa_change;
			faa_parm.last_faa = faa_parm.last_faa-1+faa_parm.faa_change;
			faa_parm.last_cache = faa_parm.last_cache+1-faa_parm.faa_change;
			GET_FPDD_DATA()->chunk_cache->CHUNK_CACHE_NUM = faa_parm.last_cache*ASSEMBLE_BUF_SIZE;

			
			if((recipe_meta->chunk_num-look_ahead_num)>=(faa_parm.law_change*ASSEMBLE_BUF_SIZE))
			{
				for(j=0;j<faa_parm.law_change;j++)
				{
					mtdata=(struct metadata *)malloc(ASSEMBLE_BUF_SIZE*sizeof(struct metadata));
					fread(mtdata,sizeof(struct metadata),ASSEMBLE_BUF_SIZE, metafp);
					add_to_look_ahead_window(mtdata, ASSEMBLE_BUF_SIZE, look_ahead_count);
					GET_FPDD_DATA()->current_look_ahead_size++;
					look_ahead_count++;
					look_ahead_num = look_ahead_num + ASSEMBLE_BUF_SIZE;
					wait_num = wait_num - ASSEMBLE_BUF_SIZE;
					free(mtdata);
					look_ahead_log("extend the look ahead window, window size: %lld, pcache size: %d\n",GET_FPDD_DATA()->current_look_ahead_size, chunk_cache_pcache_size());
				}
				faa_parm.last_law = faa_parm.last_law-1+faa_parm.law_change;
			}
			else if((recipe_meta->chunk_num-look_ahead_num)>0)
			{
				/*if it is the last part less than a window size, add it*/
				wait_num = recipe_meta->chunk_num-look_ahead_num;
				mtdata=(struct metadata *)malloc(wait_num*sizeof(struct metadata));
				fread(mtdata,sizeof(struct metadata),wait_num, metafp);
				add_to_look_ahead_window(mtdata, wait_num, look_ahead_count);
				i = 1+wait_num/ASSEMBLE_BUF_SIZE;
				GET_FPDD_DATA()->current_look_ahead_size++;
				look_ahead_count+=i;
				look_ahead_num = look_ahead_num + wait_num;
				look_ahead_log("last_window:%lu, %lu, %lu\n",wait_num, look_ahead_num, look_ahead_count);  
				free(mtdata);
				faa_parm.law_change = i;
				faa_parm.last_law = faa_parm.last_law+faa_parm.law_change;
			}
			else
			{
				look_ahead_log("no window extended: %lu, %lu, %lu\n", look_ahead_num, dedup_num, look_ahead_count);
				faa_parm.last_law-=1;
			}
					
		}
		else
		{
			if(dedup_num>0){
				a_size = restore_assemble_adaptive_faa(assemble_count, fp, &faa_parm);
				GET_FPDD_DATA()->current_look_ahead_size--;
				assemble_count++;
				dedup_num = 0;
			}
		}
		
	}

	fclose(fp);
	fclose(metafp);
	free(recipe_meta);
	
	result_log("%f ", (p_cache_size*1.0)/(stat_count*initial_cache_num));
	return 0;
}


int restore_pipe_line_faa(uint64_t chunk_num, char * file_name, int initial_look_num, int initial_faa_num, int initial_cache_num)
{
	container_log("restore_pipe_line_faa............. \n");	
	GET_FPDD_DATA()->chunk_cache->CHUNK_CACHE_NUM = initial_cache_num*ASSEMBLE_BUF_SIZE;
	GET_FPDD_DATA()->chunk_cache->p_num = 1;

	FILE * fp, * metafp;
	uint32_t a_size;
	uint64_t restored_num, dedup_num, look_ahead_num, wait_num;
	uint64_t assemble_count, look_ahead_count, next_faa_count, p_cache_size, stat_count;
	int ret;
	int i,j, current_pcache, average_ecache_in_law, cache_change;
	char metapath[PATH_MAX], restorepath[PATH_MAX];
	struct metadata * mtdata;
	struct disk_hash_node node;
	struct _faa_buf *tmp_faa_buf;

	struct _adaptive_faa_parm faa_parm;
	adaptive_faa_parm_init(&faa_parm);
	faa_parm.last_faa = initial_faa_num;
	faa_parm.last_cache = initial_cache_num;
	faa_parm.last_law = initial_look_num;
	faa_parm.effective_faa = 0;
	faa_parm.effective_cache = 0;
	
	
	/*process the metadata and restore file path*/
	strcpy(metapath,GET_FPDD_DATA()->recipedir);
	strcat(metapath,file_name);
	strcpy(restorepath,GET_FPDD_DATA()->restoredir);
	strcat(restorepath,file_name);

	struct _recipe_meta *recipe_meta;
	recipe_meta = (struct _recipe_meta *)malloc(sizeof(struct _recipe_meta));
	
	/*read out the recipe_meta and file recipe from head*/
	metafp=fopen(metapath,"r");
	if (metafp==NULL) 
	{
		error_log("Open file error:%s\n",metapath);
	}
	fseek(metafp, 0, SEEK_SET);
	fread(recipe_meta,sizeof(struct _recipe_meta),1,metafp);      //get the number of chunks
	recipe_meta->chunk_num = chunk_num;
	restored_num = 0;
	look_ahead_num = 0;
	look_ahead_count = 0;
	wait_num = recipe_meta->chunk_num;
	dedup_num = recipe_meta->chunk_num;
	
	/*add the inital recipe to the look ahead window*/
	if(recipe_meta->chunk_num>=(ASSEMBLE_BUF_SIZE*initial_look_num))
	{	
		for(i=0;i<initial_look_num;i++)
		{
			mtdata=(struct metadata *)malloc(ASSEMBLE_BUF_SIZE*sizeof(struct metadata));
			fread(mtdata,sizeof(struct metadata),ASSEMBLE_BUF_SIZE, metafp);
			add_to_look_ahead_window(mtdata, ASSEMBLE_BUF_SIZE, look_ahead_count);
			look_ahead_count++;
			look_ahead_num = look_ahead_num + ASSEMBLE_BUF_SIZE;
			GET_FPDD_DATA()->current_look_ahead_size++;
			free(mtdata);
			
	
		}
	}
	else
	{
		while(wait_num>0)
		{
			if(wait_num>=ASSEMBLE_BUF_SIZE)
			{
				mtdata=(struct metadata *)malloc(ASSEMBLE_BUF_SIZE*sizeof(struct metadata));
				fread(mtdata,sizeof(struct metadata),ASSEMBLE_BUF_SIZE, metafp);
				add_to_look_ahead_window(mtdata, ASSEMBLE_BUF_SIZE, look_ahead_count);
				look_ahead_count++;
				look_ahead_num = look_ahead_num + ASSEMBLE_BUF_SIZE;
				wait_num = wait_num - ASSEMBLE_BUF_SIZE;
				free(mtdata);
			}
			else
			{
				mtdata=(struct metadata *)malloc(wait_num*sizeof(struct metadata));
				fread(mtdata,sizeof(struct metadata),wait_num, metafp);
				add_to_look_ahead_window(mtdata, wait_num, look_ahead_count);
				look_ahead_count++;
				look_ahead_num = look_ahead_num + wait_num;
				wait_num = 0;
				free(mtdata);
			}
			GET_FPDD_DATA()->current_look_ahead_size++;
		}
		
	}

	next_faa_count = 0;
	for(i=0;i<initial_faa_num;i++)
	{
		/*add a new faa*/
		tmp_faa_buf = faa_buf_init(next_faa_count);
		faa_add_tail(tmp_faa_buf);
		next_faa_count++;
	}

	fp = fopen(restorepath, "w+");
	bool need_look_ahead;

	assemble_count = 0;

	look_ahead_log("window size: %lld, pcache size: %d, tail_count: %lld\n",GET_FPDD_DATA()->current_look_ahead_size, chunk_cache_pcache_size(), GET_FPDD_DATA()->faa->tail_count);


	p_cache_size = 0; 
	stat_count = 0;
	while(dedup_num>0)
	{
		p_cache_size+=chunk_cache_pcache_size();
		stat_count++;
		//result_log("p_cache size: %lu, e_chunk size:%lu\n", chunk_cache_pcache_size(),GET_FPDD_DATA()->chunk_cache->e_num);
		if(dedup_num>=ASSEMBLE_BUF_SIZE)
		{	
			/*assemble the current one*/
			a_size = restore_assemble_adaptive_faa(assemble_count, fp, &faa_parm);
			GET_FPDD_DATA()->current_look_ahead_size--;
			assemble_count++;
			dedup_num = dedup_num - ASSEMBLE_BUF_SIZE;
			
			/*process and delete the current faa, add a new faa*/
			tmp_faa_buf = faa_remove_head();
			faa_buf_destroy(tmp_faa_buf);
			tmp_faa_buf = faa_buf_init(next_faa_count);
			faa_add_tail(tmp_faa_buf);
			next_faa_count++;

			
			if((recipe_meta->chunk_num-look_ahead_num)>=ASSEMBLE_BUF_SIZE)
			{
					mtdata=(struct metadata *)malloc(ASSEMBLE_BUF_SIZE*sizeof(struct metadata));
					fread(mtdata,sizeof(struct metadata),ASSEMBLE_BUF_SIZE, metafp);
					add_to_look_ahead_window(mtdata, ASSEMBLE_BUF_SIZE, look_ahead_count);
					GET_FPDD_DATA()->current_look_ahead_size++;
					look_ahead_count++;
					look_ahead_num = look_ahead_num + ASSEMBLE_BUF_SIZE;
					wait_num = wait_num - ASSEMBLE_BUF_SIZE;
					free(mtdata);
					look_ahead_log("extend the look ahead window, window size: %lld, pcache size: %d\n",GET_FPDD_DATA()->current_look_ahead_size, chunk_cache_pcache_size());
			}
			else
			{
				/*if it is the last part less than a window size, add it*/
				wait_num = recipe_meta->chunk_num-look_ahead_num;
				mtdata=(struct metadata *)malloc(wait_num*sizeof(struct metadata));
				fread(mtdata,sizeof(struct metadata),wait_num, metafp);
				add_to_look_ahead_window(mtdata, wait_num, look_ahead_count);
				GET_FPDD_DATA()->current_look_ahead_size++;
				look_ahead_count++;
				look_ahead_num = look_ahead_num + wait_num;
				free(mtdata);
			}
					
		}
		else
		{
			if(dedup_num>0){
				a_size = restore_assemble_adaptive_faa(assemble_count, fp, &faa_parm);
				GET_FPDD_DATA()->current_look_ahead_size--;
				assemble_count++;
				dedup_num = 0;
			}
		}
		
	}

	fclose(fp);
	fclose(metafp);
	free(recipe_meta);
	
	result_log("%f ", (p_cache_size*1.0)/(stat_count*initial_cache_num));
	return 0;
}


	
