/*
 * optsmr.h
 * created date: 2017/02/03
 * author: Zhichao Cao
 *
*/

#ifndef _OPTSMR_H_
#define _OPTSMR_H_

struct optsmr_state {
	struct smr_info *smr_info;
	struct dedup_manager * dedup;
	struct container * container;
	struct _zone_container_table * zone_container_table;
	struct node_cache_header * write_cache;
	struct _zone_stat *zone_stat;
	struct _container_cache_table *container_cache;
	struct _chunk_cache_table *chunk_cache;
	struct _look_ahead_table *look_ahead_table;
	struct _faa *faa;
	
	FILE *logfile;


	uint64_t chunk_counter;					/*start from 1*/
	uint32_t container_counter;				/*start from 1*/
	uint32_t stream_id_limit;
	uint32_t stream_id;
	uint64_t dup_num;
	uint64_t unique_num;
	uint64_t BF_false;
	uint64_t cache_hit;
	uint64_t cache_miss;
	uint64_t total_container_read;
	uint64_t total_restore_size;			/*the restored file size*/
	uint64_t current_look_ahead_size;       /*how many assembling buffers in LAW*/

	char *metadir;
	char *dedupedchunkdir;
	char *recipedir;
	char *tracedir;
	char *restoredir;
	char *memstoredir;
	
};


struct optsmr_state *fpdd_datas;
extern struct optsmr_state *fpdd_datas;
#define GET_FPDD_DATA() fpdd_datas

char metadir[100];
extern char metadir[100];
char dedupedchunkdir[100];
extern char  dedupedchunkdir[100];
char recipedir[100];
extern char  recipedir[100];
char restoredir[100];
extern char  restoredir[100];
char memstoredir[100];
extern char  memstoredir[100];







#endif


