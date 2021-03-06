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
#ifdef COMPRESS
#include "lzjb.h"
#endif
#include "dedup.h"
#include "chunk.h"
#include "optsmr.h"
#include "chunk_cache.h"
#include "look_ahead.h"
#include "trace_work.h"
#include "direct_rw.h"
#include "smr.h"
#include "zone_allocation.h"

static int mk_dir(char *dir)
{
	DIR *mydir = NULL;
	if((mydir = opendir(dir)) == NULL)
	{
		int ret = mkdir(dir, DIR_MODE);
		if(ret != 0)
		{
			return -1;
		}
	}
	else
	{
		return -1;
	}
	return 0;
}



void _initial_global()
{
    GET_FPDD_DATA() = malloc(sizeof(struct optsmr_state));
    if (!GET_FPDD_DATA()) {
        perror("main calloc");
        exit(-1);
    }
}


void optsmr_init(void)
{

	strcpy(dedupedchunkdir, dedupedchunkdirectory);
	mk_dir(dedupedchunkdir);
	GET_FPDD_DATA()->dedupedchunkdir = dedupedchunkdir;

	strcpy(recipedir, recipedirectory);
	mk_dir(recipedir);
	GET_FPDD_DATA()->recipedir = recipedir;

	strcpy(restoredir, restoredirectory);
	mk_dir(restoredir);
	GET_FPDD_DATA()->restoredir = restoredir;

	strcpy(memstoredir, memstoredirectory);
	mk_dir(memstoredir);
	GET_FPDD_DATA()->memstoredir = memstoredir;

	strcpy(metadir,METAFILE);
	GET_FPDD_DATA()->metadir = metadir;
	GET_FPDD_DATA()->tracedir ="./trace/";
	
	GET_FPDD_DATA()->chunk_counter = 1;
	GET_FPDD_DATA()->container_counter = 1;
	GET_FPDD_DATA()->stream_id_limit = 1;
	GET_FPDD_DATA()->stream_id = 1;
	GET_FPDD_DATA()->dup_num = 0;
	GET_FPDD_DATA()->unique_num = 0;
	GET_FPDD_DATA()->BF_false = 0;
	GET_FPDD_DATA()->cache_hit = 0;
	GET_FPDD_DATA()->cache_miss = 0;
	GET_FPDD_DATA()->total_container_read = 0;
	GET_FPDD_DATA()->total_restore_size = 0;
	GET_FPDD_DATA()->current_look_ahead_size = 0;


    GET_FPDD_DATA()->logfile = log_open();
#ifdef SMR
    GET_FPDD_DATA()->smr_info = smr_init();
	if (!(GET_FPDD_DATA()->smr_info))
        smr_log("smr_init error\n");
#endif

	GET_FPDD_DATA()->dedup = deduplication_init();
	GET_FPDD_DATA()->container = container_init(GET_FPDD_DATA()->container_counter);
	GET_FPDD_DATA()->zone_container_table = zone_container_table_init();
	GET_FPDD_DATA()->write_cache = write_cache_init();
//	GET_FPDD_DATA()->zone_stat = zone_allocation_init(); 
	GET_FPDD_DATA()->container_cache = container_cache_init();
	GET_FPDD_DATA()->chunk_cache =chunk_cache_init(); 
	GET_FPDD_DATA()->look_ahead_table = look_ahead_table_init();
	GET_FPDD_DATA()->faa = faa_init();
	
	
	write_standard_container();
}

void optsmr_destroy(void)
{
	deduplication_destroy(GET_FPDD_DATA()->dedup);
	container_destroy(GET_FPDD_DATA()->container);
#ifdef SMR
    if(!smr_cleanup(GET_FPDD_DATA()->smr_info)){
        smr_log("smr_clean up error\n");
        exit -1;
    }
#endif
	zone_container_table_destroy(GET_FPDD_DATA()->zone_container_table);
//	zone_allocation_destroy(GET_FPDD_DATA()->zone_stat);
	container_cache_destroy(GET_FPDD_DATA()->container_cache);
	chunk_cache_destroy(GET_FPDD_DATA()->chunk_cache);
	look_ahead_table_destroy(GET_FPDD_DATA()->look_ahead_table);
	faa_destroy(GET_FPDD_DATA()->faa);
	
}

static int buf_to_int(char *line)
{
	int i,j,order,len;
	char data[11];
	int num=0;
	for(i=0;i<11;i++)
	{
		if(line[i]=='\n')
			break;
		data[num] = line[i];
		num++;
	}
	order = 1;
	len = 0;
	for(i=0;i<num;i++)
	{
		len = len+order*((int)(data[num-i-1]-48));
		order = order*10;
	}
	printf("len: %d\n", len);
	return len;
}

void process_trace_file_name(char *buf, char * file_name)
{
	int i, size;
	for(i=0;i<100;i++)
	{
		if(buf[i]=='\n')
		{
			printf("i: %d\n",i);
			size = i;
			break;
		}
	}
	file_name = (char *)malloc(size);
	for(i=0;i<size;i++)
	{
		printf("%c\n",buf[i]);
		file_name[i] = buf[i];
	}
	return;
}

int main(int argc, char *argv[])
{

	 _initial_global();
	optsmr_init();
	int mode;
	if(strcmp(argv[1], "container")==0)
	{
		mode = 1;
		result_log("container caching\n");
	}
	if(strcmp(argv[1], "chunk")==0)
	{
		mode = 2;
		result_log("LRU chunk caching\n");
	}
	if(strcmp(argv[1], "adaptive")==0)
	{
		mode = 3;
		result_log("adaptive chunk caching\n");
	}
	if(strcmp(argv[1], "assembly")==0)
	{
		mode = 4;
		result_log("forward assembly only\n");
	}
	if(strcmp(argv[1], "adaptive_faa")==0)
	{
		mode = 5;
//		result_log("adaptive faa designs\n");
	}
	if(strcmp(argv[1], "rw")==0)
	{
		mode = 6;
		result_log("direct read and write test on reloaded file or deduplicated file\n");
	}
	if(strcmp(argv[1], "dedup")==0)
	{
		mode = 7;
		result_log("deduplicate the file\n");
	}
	if(strcmp(argv[1], "faa_cache")==0)
	{
		mode = 8;
		//result_log("faa_cache\n");
	}
 
	/*
   int mode;
     for(mode=1;mode<4;mode++)                   
     {
    optsmr_seq_write(64, (16*1024*1024), 288000);
    int i;
    for (i=0;i<30;i++)
    {
        result_log("64-%d\n",(29100-1000*i));
        optsmr_range_read(64, (29700-1000*i), 1024,1000);
        //result_log("the range: 64-%d\n",(29700-3000*i));
    }
	*/
	int trace_num, i, j, size;
	char *file="./config";
	char line[100], trace_file[100], buf[100];
	char *file_name;
	double r_time;
	struct timeval tpstart,tpend;
	FILE *config;
	config = fopen(file, "r");
	fgets(line, 100, config);
	trace_num = buf_to_int(line);
	for(j=0;j<trace_num;j++)
	{
		fgets(trace_file,100, config);
		fgets(line,100, config);
		
		for(i=0;i<100;i++)
		{
			if(trace_file[i]=='\n')
			{
				size = i;
				break;
			}
		}
		file_name = (char *)malloc(size);
		for(i=0;i<size;i++)
		{
			file_name[i] = trace_file[i];
		}

		if(mode==7)
			dedup_trace_file(buf_to_int(line), file_name);
		if(mode==6)
		{
			if(strcmp(argv[2], "sequential")==0)
				direct_rw_sequential_test(file_name);
			if(strcmp(argv[2], "random")==0)
				direct_rw_random_test(file_name);
		}
		gettimeofday(&tpstart, NULL);
                if(mode==8)
		{
			restore_pipe_line_faa(buf_to_int(line), file_name, atoi(argv[2]), atoi(argv[3]), atoi(argv[4]));
		}
		//trace_restore_look_ahead_batch_adaptive(file_name, 12);
		if(mode==5)
		{
			restore_pipe_line_adaptive_faa(buf_to_int(line), file_name, atoi(argv[2]), atoi(argv[3]), atoi(argv[4]));
		}
		if(mode==4)
		{
			trace_restore_look_ahead(buf_to_int(line),file_name);
		}
		if(mode==3)
			restore_pipe_line_adaptive_look_ahead(buf_to_int(line),file_name, LOOK_AHEAD_TIMES);
		if((mode==1)||(mode==2))
			restore_pipe_line_look_ahead(buf_to_int(line), file_name, LOOK_AHEAD_TIMES, mode); /*1 is container, 2 is chunk cache, 3 is adaptive*/
		gettimeofday(&tpend, NULL);
		r_time=(1000000*(tpend.tv_sec-tpstart.tv_sec)+tpend.tv_usec-tpstart.tv_usec);
		

		free(file_name);
	}

	result_log("duplicated: %lld\n", GET_FPDD_DATA()->dup_num);
	result_log("unique: %lld\n", GET_FPDD_DATA()->unique_num);
	result_log("BF false positive: %lld\n",GET_FPDD_DATA()->BF_false);
	result_log("total container read: %lld\n",GET_FPDD_DATA()->total_container_read);
	result_log("restore cache miss: %lld\n",GET_FPDD_DATA()->cache_miss);
	result_log("restore cache hit: %lld\n",GET_FPDD_DATA()->cache_hit);
	result_log("restored file total size: %lld\n",GET_FPDD_DATA()->total_restore_size);
	result_log("the restore time: %f, the I/O time: %f, the computing time: %f\n", r_time/1000000, GET_FPDD_DATA()->total_container_read*(CONTAINER_IO_DELAY)/1000000.0, r_time/1000000-(GET_FPDD_DATA()->total_container_read*(CONTAINER_IO_DELAY)/1000000.0));
	result_log("the restore throughput: %f MB/S\n", (GET_FPDD_DATA()->total_restore_size/(1024.0*1024.0))/(r_time/1000000));
    result_log("Hello, world!\n");

	result_log("%lld ",GET_FPDD_DATA()->total_container_read);
	result_log("%lld ",GET_FPDD_DATA()->cache_miss);
	result_log("%lld ",GET_FPDD_DATA()->cache_hit);
	result_log("%lld ",GET_FPDD_DATA()->total_restore_size);
	result_log("%f %f %f ",r_time/1000000, GET_FPDD_DATA()->total_container_read*(CONTAINER_IO_DELAY)/1000000.0, r_time/1000000-(GET_FPDD_DATA()->total_container_read*(CONTAINER_IO_DELAY)/1000000.0));
	 result_log("%f \n", (GET_FPDD_DATA()->total_restore_size/(1024.0*1024.0))/(r_time/1000000));



	/*
	GET_FPDD_DATA()->dup_num = 0;
	GET_FPDD_DATA()->unique_num = 0;
	GET_FPDD_DATA()->BF_false = 0;
	GET_FPDD_DATA()->total_container_read = 0;
	GET_FPDD_DATA()->cache_miss = 0;
	GET_FPDD_DATA()->cache_hit = 0;
	GET_FPDD_DATA()->total_restore_size = 0;

	container_cache_destroy(GET_FPDD_DATA()->container_cache);
        chunk_cache_destroy(GET_FPDD_DATA()->chunk_cache);
	look_ahead_table_destroy(GET_FPDD_DATA()->look_ahead_table);

	GET_FPDD_DATA()->container_cache = container_cache_init();
        GET_FPDD_DATA()->chunk_cache =chunk_cache_init(); 
        GET_FPDD_DATA()->look_ahead_table = look_ahead_table_init();

    }	
	*/

    optsmr_destroy();
    return 0;
}


