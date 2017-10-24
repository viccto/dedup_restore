/*
 * metadata.h
 *
 *  Created on: 2012-7-12
 *      Author: BadBoy Tomczc
 */

#ifndef METADATA_H_
#define METADATA_H_


struct mtdata_seg_header
{
	uint64_t previous;
	uint64_t next;
};

struct metadata
{
	char fingerprint[FINGERPRINT_LEN];
	uint32_t len;
#ifdef COMPRESS
	uint32_t origin_len;
#endif
	uint64_t offset;
	int64_t zone_id;
	uint64_t container_name;
	uint64_t counter;
	bool modified;
	
};

#define MTDATA_PER_SEGMENT ((SEG_SIZE - sizeof(struct mtdata_seg_header)) / sizeof(struct metadata))

struct mtdata_seg
{
	struct mtdata_seg_header header;
	//uint64_t mtdata_offset;
	uint64_t mt_seg_offset;
	uint32_t len;
	struct storage_manager * manager;
	struct metadata mtdata[MTDATA_PER_SEGMENT];
};

int metadata_init(struct mtdata_seg * mt_seg);

uint64_t add_metadata(struct metadata mtdata, struct mtdata_seg * mt_seg);

int get_metadata(struct mtdata_seg * mt_seg, struct metadata * mtdata, int len, uint64_t offset);

int write_recipe(struct metadata *mtdata, FILE* fp);

#endif /* METADATA_H_ */
