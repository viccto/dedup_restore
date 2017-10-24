/*
 * container.h
 *
 *  Created on: 2016-4-30
 *      Author: Zhichao Cao
 */

#ifndef CONTAINER_H_
#define CONTAINER_H_

/*the index in the container head to search the chunk*/
struct chunk_index
{
	char fingerprint[FINGERPRINT_LEN];
	uint32_t offset;
	uint32_t size;
	uint64_t counter;
};

/*container head*/
struct container_header
{
	uint32_t chunk_num; 		/*current chunk number*/
	uint64_t container_name; 	/*name to be indexed by indexing table*/
	uint32_t data_size;	/*current totol size of container=head+data*/
	uint32_t data_offset;	/*the next data offset(last chunk offset+size)*/
	uint32_t stream_id; /*used for local*/
	struct chunk_index index[CHUNK_NUM];
};

#define MAX_CONTAINER_DATA (MAX_CONTAINER_SIZE-sizeof(struct container_header))

/*container it self*/
struct container
{	
	struct container_header header;
	char data[MAX_CONTAINER_DATA];
};

struct container_info
{
	uint64_t container_name;
	uint32_t zone;
};

struct _zone_container_table
{
	uint32_t zone_num;
	uint32_t *zone_total_chunk; /*current total chunk number*/
	uint32_t *zone_free_chunk;  /*current deleted chunk number*/
	uint32_t *current_pointer; /*first empty container number of this zone*/
	struct container_info **container_table;
};

struct _container_cache_node
{
	uint64_t container_name;
	char *container_ptr;
	struct _container_cache_node *lru_pre_ptr; /*pointer for lru*/
	struct _container_cache_node *lru_next_ptr; /*pointer for lru*/
	struct _container_cache_node *hm_pre_ptr; /*pointer for hashmap*/
	struct _container_cache_node *hm_next_ptr; /*pointer for hashmap*/
};

struct _container_cache_table
{
	int cached_num;
	struct _container_cache_node table[CONTAINER_CACHE_BUCK];
	struct _container_cache_node lru;
};

	
	
void write_standard_container(void);

struct container *container_init(uint64_t container_name);

void container_destroy(struct container *container);

int new_container(uint64_t container_name);

struct _zone_container_table *zone_container_table_init();

void zone_container_table_destroy(struct _zone_container_table *zone_container_table);

struct container_info **container_table_init();

void container_table_destroy(struct container_info **zone_table, uint32_t zone_num);

void increase_zone_free(uint64_t container_name);

void decrease_zone_free(uint64_t container_name);

void reset_zone_table_at_zone(int zone);

int smr_get_zone_id(uint64_t container_name);

void write_zone_container_table(struct  container_info **zone_table);

int write_container(struct container *container);

int write_container_2_smr(struct container *container);

int read_container(struct container *buf, uint64_t container_name);

int _read_container(struct container *buf, uint64_t container_name);

int read_container_from_smr(struct container *container, uint64_t container_name);

int grabe_container(char *buff, struct container_header *header, uint64_t container_name);

void release_container(char *buff);

uint64_t add_2_container(char *buff, uint32_t len, char fingerprint[FINGERPRINT_LEN], uint64_t counter, struct chunk_index *index);

int read_container_to_buf(char *buff, uint64_t container_name);

uint64_t get_chunk(char *buff, uint32_t offset, uint32_t len, char fingerprint[FINGERPRINT_LEN], uint64_t container_name);

uint64_t get_container_header(struct container_header *header, uint64_t container_name);

int container_exist_fake(uint64_t container_name);

/*for debug*/
void print_zone_table();

/*for container cache*/
struct _container_cache_table *container_cache_init(void);

void container_cache_destroy(struct _container_cache_table *container_cache);

void remove_from_container_cache(struct _container_cache_node *con_node);

uint64_t evict_from_container_cache(void);

int add_2_container_cache(char *container_ptr, uint64_t container_name);

struct _container_cache_node * find_container_cache(uint64_t container_name);

void move_mru_container_cache(struct _container_cache_node *tmp);








#endif /* CONTAINER_H_ */
