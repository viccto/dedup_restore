/* 
 * trace_work.h, it is used to handle the dedup trace
 * authoer: zhichao cao
 * created date: 02/10/2017
 */

#ifndef _TRACE_WORK_H_
#define  _TRACE_WORK_H_

struct _recipe_meta
{
	int64_t chunk_num;
	char trace_name[100];
};

int dedup_trace_file(int64_t chunk_num, char * file_name);

int trace_restore_look_ahead_batch_adaptive(char * file_name, int initial_look_num);

int trace_restore_look_ahead(uint64_t chunk_num, char * file_name);

int restore_pipe_line_adaptive_look_ahead(uint64_t chunk_num, char * file_name, int initial_look_num);

int restore_pipe_line_look_ahead(uint64_t chunk_num, char * file_name, int initial_look_num,  int cache_mode);

int restore_pipe_line_adaptive_faa(uint64_t chunk_num, char * file_name, int initial_look_num, int initial_faa_num, int initial_cache_num);

int restore_pipe_line_faa(uint64_t chunk_num, char * file_name, int initial_look_num, int initial_faa_num, int initial_cache_num);

#endif
