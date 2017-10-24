/*
 * smr.h
 * SMR drive related util functions.
 *
 * Fenggang Wu, Zhichao Cao
 * 5/15/2016
 */

#ifndef _SMR_H_
#define _SMR_H_

#define SMR_DEVICE "/dev/sdb"
#define SMR_CNR_TABLE_SIZE (4*1024*1024) /* smr container table size */
#define SMR_ZONE_SIZE (256*1024*1024)
#define SMR_LOGICAL_BLK_SIZE 512
#define SMR_LBA_PER_ZONE (SMR_ZONE_SIZE/SMR_LOGICAL_BLK_SIZE)

/************ structures *************/

/* container table entry */
struct smr_cnr_entry {
	uint64_t cnr_id; /* container id */
	uint64_t lba; /* lba in 512 Bytes. Note that the write must be
		       * 4KB aligned. zone idx and offset can be derived by
		       * this lba.
		       */
};



struct smr_info {
//struct smr_cnr_entry *smr_cnr_table; /* container to lba mapping */
	int64_t *cnr_table; /* container to lba mapping */
	struct zbc_device_info *dev_info;
	struct zbc_device *dev;
	struct zbc_zone *zones;
	unsigned int nr_zones;
        int32_t *stream2openzone; /* current open zones, 
				     * indexed by stream_id */

	int32_t *zone2stream; /* zone 2 stream_id map */
};


/*********local functions **************/
int32_t _smr_get_zone(struct smr_info*, int32_t);


/******** API  *************************/
struct smr_info* smr_init();

int smr_cleanup(struct smr_info*);

int smr_states(uint32_t*, uint32_t*);

size_t /* bytes successfully written, -1 on error */
smr_lba_write(uint64_t lba, /*the lba to write the data*/
	  uint64_t cnr_id,
      char *buf,
      size_t iosize);

size_t /* byte successfully read, -1 on error */
smr_lba_read(uint64_t lba, /*the lba to write the data*/
     uint64_t cnr_id, /*container ID*/
     char* buf, /* fixed sized buffer, space allocated by caller */
     size_t iosize);


size_t /* bytes successfully written, -1 on error */
smr_write(uint64_t cnr_id, /*container ID*/
	  char *buf,
	  size_t iosize,
	  int32_t stream_id,
	  int32_t *zoneidx); /* return value */


size_t /* byte successfully read, -1 on error */
smr_read(uint64_t cnr_id, /*container ID*/
	 char* buf, /* fixed sized buffer, space allocated by caller */
	 size_t iosize);

int32_t smr_lba_2_zone(int64_t lba);

int64_t smr_lba_zone_wp(uint64_t lba);

int32_t smr_get_zoneidx(int64_t cnr_id);

int smr_invalidate_cnr(int64_t cnr_id);

int smr_reset_pointer(int32_t zoneidx);

int smr_get_full_zones(int32_t *full_zone_array, int32_t *full_zone_cnt);

void smr_selftest();

#endif /* _SMR_H_ */
