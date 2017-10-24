/* 
 * direct_rw.c, it is used simulate the directly read and write to the deduplicated files
 * authoer: zhichao cao
 * created date: 03/26/2017
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
#include "look_ahead.h"
#include "adaptive_faa.h"
#include "trace_work.h"
#include "direct_rw.h"
#include "optsmr.h"

uint64_t range_random(uint64_t start, uint64_t end)
{
	uint64_t  ran_ret;
	double range = end-start;
	ran_ret = start+(uint64_t)(range*rand()/(RAND_MAX+1.0));
	return ran_ret;
	
}


int direct_rw_sequential_test(char * file_name)
{
	uint64_t size, offset, chunk_size, read_size, write_size;
	char *buf;
	double r_time;
	struct timeval tpstart,tpend;
	
	size = 20*1024*1024;
	offset = 0;
	chunk_size = 4*1024;
	buf = malloc(size);

	/*read test*/
	gettimeofday(&tpstart, NULL);
	read_size = read_from_reloaded(buf, size, offset, chunk_size, file_name);	
	gettimeofday(&tpend, NULL);
	r_time=(1000000*(tpend.tv_sec-tpstart.tv_sec)+tpend.tv_usec-tpstart.tv_usec);

	result_log("the sequential read size: %d\n", read_size);
	result_log("the sequential read time: %f\n", r_time/1000000);
	result_log("the sequential read throughput: %f MB/S\n", (size/(1024.0*1024.0))/(r_time/1000000));

	/*write test*/
	memset(buf, 'a', size);
	gettimeofday(&tpstart, NULL);
	write_size = write_to_reloaded(buf, size, offset, chunk_size, file_name);	
	gettimeofday(&tpend, NULL);
	r_time=(1000000*(tpend.tv_sec-tpstart.tv_sec)+tpend.tv_usec-tpstart.tv_usec);

	result_log("the write size: %d\n", write_size);
	result_log("the write time: %f\n", r_time/1000000);
	result_log("the write throughput: %f MB/S\n", (size/(1024.0*1024.0))/(r_time/1000000));

	free(buf);
	return 0;
}

int direct_rw_random_test(char * file_name)
{
	uint64_t size, offset, chunk_size, read_size, write_size, i, tmp;
	char *buf;
	double r_time;
	struct timeval tpstart,tpend;

	size = 1*1024*1024;
	buf = malloc(size);
	chunk_size = 4*1024;

	read_size = 0;

	gettimeofday(&tpstart, NULL);
	for(i=0;i<32;i++)
	{
		offset = range_random(0,100*1024*1024);
		tmp = read_from_reloaded(buf, size, offset, chunk_size, file_name);
		read_size+=size;
	}
	gettimeofday(&tpend, NULL);
	r_time=(1000000*(tpend.tv_sec-tpstart.tv_sec)+tpend.tv_usec-tpstart.tv_usec);

	result_log("the random read total size: %lld\n", read_size);
	result_log("the random read time: %f\n", r_time/1000000);
	result_log("the random read throughput: %f MB/S\n", (read_size/(1024.0*1024.0))/(r_time/1000000));

	write_size = 0;
	memset(buf, 'a', size);
	gettimeofday(&tpstart, NULL);
	for(i=0;i<32;i++)
	{
		offset = range_random(0,100*1024*1024);
		tmp = write_to_reloaded(buf, size, offset, chunk_size, file_name);
		write_size+=size;
	}
	gettimeofday(&tpend, NULL);
	r_time=(1000000*(tpend.tv_sec-tpstart.tv_sec)+tpend.tv_usec-tpstart.tv_usec);

	result_log("the random write total size: %lld\n", write_size);
	result_log("the random write time: %f\n", r_time/1000000);
	result_log("the random write throughput: %f MB/S\n", (write_size/(1024.0*1024.0))/(r_time/1000000));
	
	free(buf);
	return 0;
}






	

/*read data from deduplicated files directly without restore
 */
int read_from_reloaded(char *buf, uint64_t size, uint64_t offset, uint64_t chunk_size, char * file_name)
{


	FILE * fp, * metafp;
	uint64_t chunk_num, start_chunk, end_chunk, seek_offset;
	int i;
	char metapath[PATH_MAX], restorepath[PATH_MAX];
	char *a_buf;
	struct metadata * mtdata;
	struct disk_hash_node node;
	struct _faa_buf *tmp_faa_buf;
	
	/*process the metadata and restore file path*/
	strcpy(metapath,GET_FPDD_DATA()->recipedir);
	strcat(metapath,file_name);
	strcpy(restorepath,GET_FPDD_DATA()->restoredir);
	strcat(restorepath,file_name);

	struct _recipe_meta *recipe_meta;
	recipe_meta = (struct _recipe_meta *)malloc(sizeof(struct _recipe_meta));
	
	/*read out the recipe_meta and file recipe from head*/
	metafp=fopen(metapath,"r");
	fp = fopen(restorepath, "w+");
	if (metafp==NULL) 
	{
		error_log("Open file error:%s\n",metapath);
	}

	/*find the corresponding chunk in the file recipe and read out*/
	start_chunk = offset/chunk_size;
	end_chunk = (offset+size)/chunk_size+1;
	chunk_num = end_chunk-start_chunk;
	look_ahead_log("start_chunk %lld, end_chunk: %lld, chunk_num: %lld\n", start_chunk, end_chunk, chunk_num);
	a_buf = malloc(MAX_CHUNK_LEN*chunk_num);	
	fseek(metafp, 0, SEEK_SET);
	fread(recipe_meta,sizeof(struct _recipe_meta),1,metafp);      //get the number of chunks
	look_ahead_log("the first seek location: %d, chunk_num: %lld\n", sizeof(struct _recipe_meta), recipe_meta->chunk_num);
	mtdata = (struct metadata *)malloc(chunk_num*sizeof(struct metadata));
	seek_offset = (sizeof(struct _recipe_meta)+start_chunk*sizeof(struct metadata));
	look_ahead_log("the seek location: %lld\n",seek_offset);
	fseek(metafp,seek_offset, SEEK_SET);
	fread(mtdata,sizeof(struct metadata),chunk_num, metafp);

	/*constrcut the temperal look ahead window*/
	struct _container_info *table_head;
	uint32_t a_buf_size;
	table_head = (struct _container_info *)malloc(sizeof(struct _container_info));
	table_head->container_next = NULL;
	table_head->chunk_next = NULL;
	table_head->container_name = 0;
	
	a_buf_size = construct_restore_table(mtdata, chunk_num, table_head);
	a_buf_size = restore_from_table(chunk_num, table_head, a_buf, fp);
	memcpy(buf, a_buf+(offset%chunk_size), size);
	
	fclose(fp);
	fclose(metafp);
	free(table_head);
	free(recipe_meta);
	free(mtdata);
	free(a_buf);
	
	return a_buf_size;
}


/*simulate the write on reloaded file*/
int write_to_reloaded(char *buf, uint64_t size, uint64_t offset, uint64_t chunk_size, char * file_name)
{
	
	FILE * fp, * metafp;
	uint64_t chunk_num, start_chunk, end_chunk, seek_offset;
	uint64_t a_buf_size;
	int i;
	char metapath[PATH_MAX], restorepath[PATH_MAX];
	char name[100];
	char *a_buf, *container_buf;
	struct metadata * mtdata;
	struct disk_hash_node node;
	struct _faa_buf *tmp_faa_buf;
	
	/*process the metadata and restore file path*/
	strcpy(metapath,GET_FPDD_DATA()->recipedir);
	strcat(metapath,file_name);


	struct _recipe_meta *recipe_meta;
	recipe_meta = (struct _recipe_meta *)malloc(sizeof(struct _recipe_meta));
	
	/*read out the recipe_meta and file recipe from head*/
	metafp=fopen(metapath,"r+");
	if (metafp==NULL) 
	{
		error_log("Open file error:%s\n",metapath);
	}

	/*find the corresponding chunk in the file recipe and read out*/
	start_chunk = offset/chunk_size;
	end_chunk = (offset+size)/chunk_size+1;
	chunk_num = end_chunk-start_chunk;
	a_buf = malloc(MAX_CHUNK_LEN*chunk_num);	
	fseek(metafp, 0, SEEK_SET);
	fread(recipe_meta,sizeof(struct _recipe_meta),1,metafp);      //get the number of chunks
	mtdata = (struct metadata *)malloc(chunk_num*sizeof(struct metadata));
	seek_offset = (sizeof(struct _recipe_meta)+start_chunk*sizeof(struct metadata));
	look_ahead_log("the seek location: %lld\n",seek_offset);
	fseek(metafp,seek_offset, SEEK_SET);
	fread(mtdata,sizeof(struct metadata),chunk_num, metafp);

	a_buf_size = 0;
	container_buf = malloc(MAX_CONTAINER_DATA);
	read_container_to_buf(container_buf, mtdata[0].container_name);
	memcpy(a_buf, container_buf+mtdata[0].offset, (offset%chunk_size));
	a_buf_size = offset%chunk_size;
	memcpy(a_buf+(offset%chunk_size), buf, size);
	a_buf_size = a_buf_size+size;
	read_container_to_buf(container_buf, mtdata[chunk_num-1].container_name);
	memcpy(a_buf+(offset%chunk_size)+size, container_buf+mtdata[chunk_num-1].offset+((offset+size)%chunk_size), chunk_size-((offset+size)%chunk_size));
	a_buf_size = a_buf_size+chunk_size-((offset+size)%chunk_size);

	uint64_t write_size, buf_start=0;
	for(i=0;i<a_buf_size/chunk_size;i++)
	{
		if(a_buf_size-buf_start>chunk_size)
			write_size = chunk_size;
		else
			write_size = a_buf_size-buf_start;

		strcpy(restorepath,GET_FPDD_DATA()->restoredir);
		sprintf(name, "/%d", GET_FPDD_DATA()->chunk_counter);
		GET_FPDD_DATA()->chunk_counter++;
		strncat(restorepath,name, PATH_MAX);
		fp = fopen(restorepath, "w+");
		if(fp==NULL)
		{
			error_log("chunk write failed!\n");
			return -1;
		}
		fwrite(a_buf+buf_start, 1, write_size, fp);
		fflush(fp);
		fclose(fp);
		buf_start +=write_size;
		mtdata[i].container_name = GET_FPDD_DATA()->chunk_counter;
	}
	fseek(metafp, (sizeof(struct _recipe_meta)+start_chunk*sizeof(struct metadata)), SEEK_SET);
	fwrite(mtdata,sizeof(struct metadata),chunk_num, metafp);
	fflush(metafp);

	fclose(metafp);
	free(container_buf);
	free(a_buf);
	free(mtdata);
	return a_buf_size;
}

	
	





