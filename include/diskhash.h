/*
 * disk-hash.h
 *
 *  Created on: 2011-7-20
 *      Author: badboy
 */

#ifndef DISK_HASH_H_
#define DISK_HASH_H_



struct disk_hash_seg_header
{
	uint64_t previous;
	uint64_t next;
};

struct disk_hash_node
{
	pthread_spinlock_t node_lock;
	char fingerprint[FINGERPRINT_LEN];
	uint32_t data_len;
	uint64_t data_offset;
	uint64_t container_name;
	uint64_t counter;
	uint64_t t1_count;
	uint64_t t2_count;
	int64_t zone_id;
	bool deleted;
};

#define DISKHASH_PER_SEG ((SEG_SIZE - sizeof(struct disk_hash_seg_header))/ sizeof(struct disk_hash_node))

struct hash_bucket
{
	struct disk_hash_seg_header header;
	uint32_t seg_stored_len;
	uint32_t len;
	uint64_t write_offset;
	uint64_t cur_seg_offset;
	struct disk_hash_node disk_hash_node[MEM_HASH_NUM];
};

struct disk_hash
{
	struct storage_manager * manager;
	char read_seg[SEG_SIZE];
	struct hash_bucket hash_bucket[BUKET_NUM];
};

int disk_hash_init(struct disk_hash * disk_hash);

int add_2_disk_hash(struct disk_hash * disk_hash, struct disk_hash_node * disk_hash_node);

int add_2_disk_hash_table(struct disk_hash * disk_hash, struct disk_hash_node *disk_hash_node);

int lookup_fingerprint_in_disk_hash(struct disk_hash * disk_hash, char fingerprint[FINGERPRINT_LEN], struct disk_hash_node * disk_hash_node);

struct disk_hash_node * lookup_fingerprint_in_disk_hash_ptr(struct disk_hash * disk_hash, char fingerprint[FINGERPRINT_LEN], int * file);


#endif /* DISK_HASH_H_ */
