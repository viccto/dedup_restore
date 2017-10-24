#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <ulockmgr.h>
#include <pthread.h>

#include "enums.h"
#include "config.h"
#include "storagemanager.h"
#include "file.h"
#include "diskhash.h"
#include "nodecache.h"
#include "data.h"
#include "container.h"
#include "list.h"
#include "metadata.h"
#include "cache.h"
#include "sha1.h"
#include "bloomfilter.h"
#include "memstore.h"
#ifdef COMPRESS
#include "lzjb.h"
#endif
#include "dedup.h"
#include "chunk.h"
#include "optsmr.h"

#ifdef DEBUG
uint64_t dup_len = 0;
uint64_t total_len = 0;
uint64_t unique_len = 0;
#ifdef COMPRESS
uint64_t compressed_len = 0;
uint64_t comp_unique_len = 0;
#endif
#endif


int dedup_init(struct dedup_manager * dedup, char * meta_file_name, char *chunk_file_name)
{
	if(NULL == dedup)
		return -1;
	storage_manager_init(&(dedup->manager), meta_file_name);
	//storage_manager_init(&(dedup->chunk_manager), chunk_file_name);
	dedup->f_seg.manager = &(dedup->manager);
	dedup->disk_hash.manager = &(dedup->manager);
	dedup->dt_seg.manager = &(dedup->chunk_manager);
	dedup->mt_seg.manager = &(dedup->manager);
	dedup->bloom = bloom_init();
	dedup->memstore = memstore_init();
	if(file_init(&(dedup->f_seg)) < 0)
		return -1;
	if(cache_init(&(dedup->cache)) < 0)
		return -1;
	if(disk_hash_init(&(dedup->disk_hash)) < 0)
		return -1;
	if(bloomfilter_init(dedup->bf) < 0)
		return -1;
	if(data_init(&(dedup->dt_seg)) < 0)
		return -1;
	if(metadata_init(&(dedup->mt_seg)) < 0)
		return -1;
	return 0;
}

int load_cache(struct mtdata_seg * seg, struct metadata * mtdata, uint64_t offset, struct cache * cache)
{
	int len;
	len = get_metadata(seg, mtdata, TO_CACHE_LEN, offset);
	add_metadata_in_cache(mtdata, len, cache);
	return 0;
}



/* generate the fingerprint of the data chunk
 */
int generate_fingerprint(char * buf, uint32_t len, struct dedup_manager * dedup, struct metadata *mtdata)
{
	dedup_log("enter generate_fingerprint..........\n");

	sha1((unsigned char *)buf, len, (unsigned char *)mtdata->fingerprint);

	mtdata->len = len;
	return 0;
}

int index_dedup(char * buf, struct metadata *mtdata, struct dedup_manager * dedup, FILE *fp)
{

#ifdef MEMSTORE
	return memstore_index_dedup(buf, mtdata, dedup, fp);
#else
	return hash_index_dedup(buf, mtdata, dedup, fp);
#endif

}


/*check the indexing table
 * do write the chunk to container
 * update the indexing table and file recipe */
int hash_index_dedup(char * buf, struct metadata *mtdata, struct dedup_manager * dedup, FILE *fp)
{
	struct disk_hash_node node;
	struct chunk_index index;
	int ret, ret1;
	uint64_t counter;
	uint64_t container_name;

	if((GET_FPDD_DATA()->dup_num+GET_FPDD_DATA()->unique_num)%10000==0)
   	 {
        	result_log("%lld %lld %f\n",GET_FPDD_DATA()->dup_num, GET_FPDD_DATA()->unique_num, ((double)(GET_FPDD_DATA()->dup_num+GET_FPDD_DATA()->unique_num))/GET_FPDD_DATA()->unique_num);
   	 }


	//ret = bloom_filter_lookup(dedup->bf, (unsigned int *) mtdata->fingerprint);
	ret = bloom_check(GET_FPDD_DATA()->dedup->bloom, mtdata->fingerprint);
	if(ret==0)
		bloom_add(GET_FPDD_DATA()->dedup->bloom, mtdata->fingerprint);
	/*
	ret1 = lookup_fingerprint_in_disk_hash(&dedup->disk_hash, mtdata->fingerprint, &node);
	if (ret!=ret1)
		result_log("%d, %d\n",ret, ret1);
	*/
	if(ret==1)
	{
		ret = lookup_fingerprint_in_disk_hash(&dedup->disk_hash, mtdata->fingerprint, &node);
		if(ret==0){
			GET_FPDD_DATA()->BF_false = GET_FPDD_DATA()->BF_false+1;
			dedup_log("the BF false possitive: %ld, %ld, %ld\n",GET_FPDD_DATA()->dup_num, GET_FPDD_DATA()->unique_num, GET_FPDD_DATA()->BF_false);
		}
		if(ret==1)
		{
			GET_FPDD_DATA()->dup_num = GET_FPDD_DATA()->dup_num+1;
			dedup_log("the dup_num: %ld\n",GET_FPDD_DATA()->dup_num);

			container_log("existing chunk, chunk_id: %lld, container_id: %lld, t2_count %lld, node pointer:%p, fingerprint: %s\n", node.counter, node.container_name, node.t2_count, &node, mtdata->fingerprint);
			mtdata->offset = node.data_offset;
			mtdata->zone_id = node.zone_id;
			mtdata->counter = node.counter;
			mtdata->container_name = node.container_name;
			write_recipe(mtdata, fp);  /*write metadata to file recipe*/
		}
	}
	if(ret==0)
	{
		dedup_log("unique chunk: %s\n",mtdata->fingerprint);
		GET_FPDD_DATA()->unique_num = GET_FPDD_DATA()->unique_num+1;
		dedup_log("the unique_num: %ld\n",GET_FPDD_DATA()->unique_num);
		counter = GET_FPDD_DATA()->chunk_counter;
		GET_FPDD_DATA()->chunk_counter++;
		container_name = add_2_container(buf, mtdata->len, mtdata->fingerprint, counter, &index);

		memcpy(node.fingerprint, mtdata->fingerprint, FINGERPRINT_LEN+1);
		node.data_len = mtdata->len;
		node.data_offset = index.offset;
		node.container_name = container_name;
		node.counter = counter;
        node.zone_id = 0;

		mtdata->offset = index.offset;
		mtdata->container_name = container_name;
		mtdata->counter = counter;
		mtdata->zone_id = node.zone_id;

		//container_log("before add New chunk, chunk_id: %lld, container_id: %lld, t1_count: %d\n", node->counter, container_name,  node->t1_count);
		
		add_2_disk_hash(&dedup->disk_hash, &node);
		write_recipe(mtdata, fp);
	}
	return ret;
}


/*using the memstore to manage the indexing table
 * not the original hash table based on*/
int memstore_index_dedup(char * buf, struct metadata *mtdata, struct dedup_manager * dedup, FILE *fp)
{
	struct disk_hash_node node;
	struct chunk_index index;
	struct mem_node *memnode;
	int ret, ret1;
	uint64_t counter;
	uint64_t container_name;

	if((GET_FPDD_DATA()->dup_num+GET_FPDD_DATA()->unique_num)%10000==0)
   	 {
        	result_log("%lld %lld %f\n",GET_FPDD_DATA()->dup_num, GET_FPDD_DATA()->unique_num, ((double)(GET_FPDD_DATA()->dup_num+GET_FPDD_DATA()->unique_num))/GET_FPDD_DATA()->unique_num);
   	 }
	
	memnode = memstore_check(GET_FPDD_DATA()->dedup->memstore, mtdata->fingerprint);
	
	/*
	ret1 = bloom_check(GET_FPDD_DATA()->dedup->bloom, mtdata->fingerprint);
	if(ret1==0)
		bloom_add(GET_FPDD_DATA()->dedup->bloom, mtdata->fingerprint);
	if(ret1!=ret)
		memstore_log("the memstore miss check:%d, %d\n", ret1, ret);

	*/
	if(memnode==NULL)
		ret =0;
	else
		ret = 1;
	

	if(memnode!=NULL)
	{
		GET_FPDD_DATA()->dup_num = GET_FPDD_DATA()->dup_num+1;
		dedup_log("the dup_num: %ld\n",GET_FPDD_DATA()->dup_num);

			//container_log("existing chunk, chunk_id: %lld, container_id: %lld, t2_count %lld, node pointer:%p, fingerprint: %s\n", node.counter, node.container_name, node.t2_count, &node, mtdata->fingerprint);
		mtdata->offset = memnode->hash_node.data_offset;
		mtdata->zone_id = memnode->hash_node.zone_id;
		mtdata->counter = memnode->hash_node.counter;
		mtdata->container_name = memnode->hash_node.container_name;
		write_recipe(mtdata, fp);  /*write metadata to file recipe*/
	}
	else
	{
		dedup_log("unique chunk: %s\n",mtdata->fingerprint);
		GET_FPDD_DATA()->unique_num = GET_FPDD_DATA()->unique_num+1;
		dedup_log("the unique_num: %ld\n",GET_FPDD_DATA()->unique_num);
		counter = GET_FPDD_DATA()->chunk_counter;
		GET_FPDD_DATA()->chunk_counter++;
		container_name = add_2_container(buf, mtdata->len, mtdata->fingerprint, counter, &index);

		memcpy(node.fingerprint, mtdata->fingerprint, FINGERPRINT_LEN+1);
		node.data_len = mtdata->len;
		node.data_offset = index.offset;
		node.container_name = container_name;
		node.counter = counter;
        node.zone_id = 0;

		mtdata->offset = index.offset;
		mtdata->container_name = container_name;
		mtdata->counter = counter;
		mtdata->zone_id = node.zone_id;

		//container_log("before add New chunk, chunk_id: %lld, container_id: %lld, t1_count: %d\n", node->counter, container_name,  node->t1_count);
		
		memstore_add(GET_FPDD_DATA()->dedup->memstore, &node);
		write_recipe(mtdata, fp);
	}
	return ret;
}


/*called by newly_chunk*/
int newly_dedup(char * buf, int len, struct dedup_manager * dedup, FILE *fp)
{
	struct metadata *mtdata;
	mtdata = (struct metadata*)malloc(sizeof(struct metadata));
	generate_fingerprint(buf, len, dedup, mtdata);
	index_dedup(buf, mtdata, dedup, fp);

	free(mtdata);
	return 0;
}







int destroy(struct dedup_manager * dedup)
{
	fclose(dedup->manager.f);
	fclose(dedup->chunk_manager.f);
	return 0;
}

struct dedup_manager *deduplication_init(void)
{
	struct dedup_manager *dedup;
	dedup = malloc(sizeof(struct dedup_manager));
	if(dedup == NULL)
		error_log("deduplication_init error!\n");
	dedup_init(dedup, GET_FPDD_DATA()->metadir, GET_FPDD_DATA()->dedupedchunkdir);
	return dedup;
}

void deduplication_destroy(struct dedup_manager *dedup)
{
	bloom_destroy(dedup->bloom);
	memstore_destroy(dedup->memstore);
	free(dedup);
	return;
}





