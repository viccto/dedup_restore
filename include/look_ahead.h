/*
 * look_ahead.h
 *
 *  Created on: 2016-12-30
 *      Author: zhichao cao
 */

#ifndef LOOK_AHEAD_H_
#define LOOK_AHEAD_H_

struct chunk_info
{
#ifdef COMPRESS
	uint32_t origin_len;
#endif
	uint32_t len;
	uint64_t offset;
	uint64_t container_name;
	uint64_t chunk_name;
	uint32_t a_buf_pos;
	struct chunk_info *next;
	struct chunk_info *pre;
	struct chunk_info *look_next;
	struct chunk_info *look_pre;
	uint64_t ab_offset;   /*when one LAW forms, it is the AB possition */
};

struct _container_info
{
	uint64_t container_name;
	struct chunk_info *chunk_next;
	struct chunk_info *chunk_pre;
	struct _container_info *container_next;
};

struct _look_ahead_table
{
	struct _container_info *table_head; 
	struct chunk_info chunk_info_hash[CHUNK_INFO_HASH_NUM];
};

bool can_do_look_ahead(void);
	
struct _look_ahead_table *look_ahead_table_init(void);

void look_ahead_table_destroy(struct _look_ahead_table *table);

bool chunk_used_again_in_window(struct chunk_info * tmp_chunk);

void add_chunk_info_hash(struct chunk_info * tmp);

void remove_chunk_info_hash(struct chunk_info *tmp);
	
uint32_t assemble_buf(char *a_buf, struct metadata * mtdata, struct dedup_manager * dedup, uint64_t restored_num, FILE *file);

uint32_t regular_assemble(char *a_buf, struct metadata * mtdata, struct dedup_manager * dedup, uint64_t restored_num, FILE *file);

uint32_t look_ahead_assemble(char *a_buf, struct metadata * mtdata, struct dedup_manager * dedup, uint64_t restored_num, FILE *file);

void add_to_look_ahead_window(struct metadata * mtdata, uint64_t restored_num, uint64_t look_ahead_count);

int restore_assemble_buf_write(char *a_buf, uint64_t dedup_num, uint64_t assemble_count,FILE *fp);

int restore_assemble_pipe_line_buf_write(char *a_buf, uint64_t dedup_num, uint64_t assemble_count,FILE *fp, int cache_mode);

uint32_t restore_assemble_pipe_line_container_cache(char *a_buf, uint64_t dedup_num, uint64_t assemble_count,FILE *fp);

int restore_assemble_pipe_line_chunk_cache(char *a_buf, uint64_t dedup_num, uint64_t assemble_count,FILE *fp);

uint32_t construct_restore_table(struct metadata * mtdata, uint64_t restored_num, struct _container_info *table_head);

uint32_t restore_from_table(uint64_t restored_num, struct _container_info *table_head, char *a_buf, FILE *file);

uint32_t restore_from_table_container_cache(uint64_t restored_num, struct _container_info *table_head, char *a_buf, FILE *file);

uint32_t restore_from_table_chunk_cache(uint64_t restored_num, struct _container_info *table_head, char *a_buf, FILE *fp);

uint32_t restore_from_table_chunk_cache_full(uint64_t restored_num, struct _container_info *table_head, char *a_buf, FILE *fp);

uint32_t restore_from_table_chunk_cache_batch(uint64_t restored_num, struct _container_info *table_head, char *a_buf, FILE *fp, int look_ahead_times, int *current_pcache);


uint32_t assemble_buf_batch_adaptive(char *a_buf, struct metadata * mtdata, struct dedup_manager * dedup, uint64_t restored_num, FILE *fp, int look_ahead_times, int *current_pcache);




#endif
