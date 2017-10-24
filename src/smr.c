#include <libzbc/zbc.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "enums.h"
#include "config.h"
#include "optsmr.h"
#include "smr.h"


/*lba to the zone number*/
int32_t smr_lba_2_zone(int64_t lba)
{
	smr_log("enter the smr_lba_2_zone, lba=:%lld\n", lba);
	int32_t zoneidx; 
	zoneidx = lba/SMR_LBA_PER_ZONE;
	return zoneidx;
}


/* return the write pointer location near in 
 * the zone the lba is going to write
 */
int64_t smr_lba_zone_wp(uint64_t lba)
{
	int32_t zoneidx;
	struct smr_info* smr_info = GET_FPDD_DATA()->smr_info;
	struct zbc_zone *iozone;

	zoneidx = smr_lba_2_zone(lba);
	iozone = &smr_info->zones[zoneidx];
	return zbc_zone_wp_lba(iozone);
}


int32_t /* zoneidx */
_smr_get_zone(struct smr_info *smr_info, int32_t stream_id) 
{
	smr_log("enter _smr_get_zone, stream_id=%d\n", stream_id);
	int32_t zoneidx = smr_info->stream2openzone[stream_id];

	smr_log("                     zoneidx was %d\n", zoneidx);
	if (zoneidx < 0 || zbc_zone_full(&smr_info->zones[zoneidx])) {
		/*
		 * FIXME:
		 * better algorithm to find a empty zone
		 */
		zoneidx = 64;

		while (zbc_zone_full(&smr_info->zones[zoneidx]) || 
		       zbc_zone_imp_open(&smr_info->zones[zoneidx]) || 
		       zbc_zone_exp_open(&smr_info->zones[zoneidx]))
			zoneidx++;
	}

	smr_info->stream2openzone[stream_id] = zoneidx;
	smr_log("                     zoneidx is  %d\n", zoneidx);
	return zoneidx;
}

struct smr_info* smr_init()
{
	smr_log("do the smr_init\n");
        int i, ret;

	struct smr_info *smr_info = malloc(sizeof(struct smr_info));


	if (!smr_info) {
		smr_log("cannot malloc for smr_info\n");
		goto err_out;
	}

	/* open device and get info from the drive */
	ret = zbc_open(SMR_DEVICE, O_WRONLY, &smr_info->dev);
	if (ret){
		smr_log("smr_init:zbc_open\n");
		goto err_out;

	}

	smr_info->dev_info = malloc(sizeof(struct zbc_device_info));

	if (!smr_info->dev_info) {
		smr_log("cannot malloc for zbc_device_info\n");
		goto err_out;
	}

	ret = zbc_get_device_info(smr_info->dev, smr_info->dev_info);
	if (ret < 0){
		smr_log("zbc_get_device_info failed\n");
		goto err_out;
	}
	
	/* Get zone list */
	ret = zbc_list_zones(smr_info->dev, 0, ZBC_RO_ALL, 
			     &smr_info->zones, &smr_info->nr_zones);
	if(ret){
		smr_log("zbc_list_zones failed\n");
		goto err_out;
	}


	/* init container table */
	smr_info->cnr_table = malloc(SMR_CNR_TABLE_SIZE * 
				     sizeof(int64_t));
	if (!smr_info->cnr_table) {
		smr_log("cannot malloc for smr_cnr_table\n");
		goto err_out;
	}

	for (i = 0; i < SMR_CNR_TABLE_SIZE; i++) {
		smr_info->cnr_table[i] = -1; /* init to invalid lba */
	}

	/* init streamID to openzone map */
	smr_info->stream2openzone = malloc(
		(unsigned int) smr_info->dev_info->zbd_opt_nr_open_seq_pref *
		sizeof(int32_t));

	if (!smr_info->stream2openzone) {
		smr_log("cannot malloc for smr_info->stream2openzone\n");
		goto err_out;
	}

	for (i = 0; i < (unsigned int) smr_info->dev_info->zbd_opt_nr_open_seq_pref; i++) {
		smr_info->stream2openzone[i] = -1; /* init to invalid zoneidx */
	}

	/* init zone to streamID map */
	smr_info->zone2stream = malloc(smr_info->nr_zones * sizeof(int32_t));

	if (!smr_info->zone2stream) {
		smr_log("cannot malloc for smr_info->zone2stream\n");
		goto err_out;
	}

	for(i = 0; i < smr_info->nr_zones; i++)
		smr_info->zone2stream[i] = -1; /* init to invalid zoneidx */

#ifdef SMR_DEBUG
	smr_log("Device %s: %s\n",
		SMR_DEVICE,
		smr_info->dev_info->zbd_vendor_id);
	smr_log("    %s interface, %s disk model\n",
		zbc_disk_type_str(smr_info->dev_info->zbd_type),
		zbc_disk_model_str(smr_info->dev_info->zbd_model));
	smr_log("    %llu logical blocks of %u B\n",
		(unsigned long long) smr_info->dev_info->zbd_logical_blocks,
		(unsigned int) smr_info->dev_info->zbd_logical_block_size);
	smr_log("    %llu physical blocks of %u B\n",
		(unsigned long long) smr_info->dev_info->zbd_physical_blocks,
		(unsigned int) smr_info->dev_info->zbd_physical_block_size);
	smr_log("    %.03F GB capacity\n",
		(double) (smr_info->dev_info->zbd_physical_blocks * 
			  smr_info->dev_info->zbd_physical_block_size) / 
		1000000000);

/* Print zone info */
	smr_log("    %u zone%s in total\n",
		smr_info->nr_zones,
		(smr_info->nr_zones > 1) ? "s" : "");
	if ( smr_info->dev_info->zbd_model == ZBC_DM_HOST_MANAGED ) {
		smr_log("    Maximum number of "
			"open sequential write required zones: %u\n",
			(unsigned int) smr_info->dev_info->
			zbd_max_nr_open_seq_req);
	} else {
		smr_log("    Optimal number of "
			"open sequential write preferred zones: %u\n",
			(unsigned int) smr_info->dev_info->
			zbd_opt_nr_open_seq_pref);
		smr_log("    Optimal number of "
			"non-sequentially written sequential write "
			"preferred zones: %u\n",
			(unsigned int) smr_info->dev_info->
			zbd_opt_nr_non_seq_write_seq_pref);
	}

#endif /* SMR_DEBUG */

	return smr_info;

err_out:
	if (smr_info->dev)
		zbc_close(smr_info->dev);
	if (smr_info->dev_info)
		free(smr_info->dev_info);
	if (smr_info->zones)
		free(smr_info->zones);
	if (smr_info->cnr_table)
		free(smr_info->cnr_table);
	if (smr_info->stream2openzone)
		free(smr_info->stream2openzone);
	if (smr_info->zone2stream)
		free(smr_info->zone2stream);
	if (smr_info)
		free(smr_info);
	return NULL;
}


int smr_cleanup(struct smr_info* smr_info)
{
	int ret;
	if (smr_info->dev){
/*
		if ((ret = zbc_reset_write_pointer_all(smr_info->dev))) {
			smr_log("warning: unable to reset all pointers, err=%d\n", ret);
		}
*/
		zbc_close(smr_info->dev);
	}
	if (smr_info->dev_info)
		free(smr_info->dev_info);
	if (smr_info->zones)
		free(smr_info->zones);
	if (smr_info->cnr_table)
		free(smr_info->cnr_table);
	if (smr_info->stream2openzone)
		free(smr_info->stream2openzone);
	if (smr_info->zone2stream)
		free(smr_info->zone2stream);
	if (smr_info)
		free(smr_info);
	return 0;
}


int  /* err code */
smr_states(uint32_t* zone_num, uint32_t* zone_size)
{
	struct smr_info* smr_info = GET_FPDD_DATA()->smr_info;
#ifdef SMR
	*zone_size = SMR_ZONE_SIZE;
	*zone_num = (uint32_t)smr_info->nr_zones;
#else
	*zone_size = SMR_ZONE_SIZE;
	*zone_num = ZONE_NUM_FAKE;
#endif
	smr_log("exit smr_states\n");
	return 0;
}




/* write the iosize data to the lba, and you and indicate 
 * the cnr_id of the data. We assume that before you call
 * this function, the lba is at the write pointer location
 */
size_t
smr_lba_write(uint64_t lba, /*the lba to write the data*/
	  uint64_t cnr_id,
	  char *buf, 
	  size_t iosize) /* return value */
{
	smr_log("smr_lba_write, lba: %ld, iosize: %d\n", lba, iosize);
	struct zbc_zone *iozone;
	size_t ioalign;
	size_t nbytes;
	int64_t lba_count, lba_ofst, start_lba, total_lba, lba_left, lba_max;
	int32_t nlba, zoneidx;
	char* bufofst;

	struct smr_info* smr_info = GET_FPDD_DATA()->smr_info;
	if (!buf)
		goto err_out;

	zoneidx = smr_lba_2_zone(lba);

	smr_log("_smr_get_zone returns %d\n", zoneidx);

	
        /* Get target zone */
	if ( zoneidx >= (int)smr_info->nr_zones ) {
		smr_log("Target zone not found\n");
		nbytes = -1;
		goto err_out;
	}
	iozone = &smr_info->zones[zoneidx];

	start_lba = zbc_zone_wp_lba(iozone);
	if(start_lba!=lba)
		error_log("the lba is not at write_pointer!\n");


#ifdef SMR_DEBUG
	smr_log("Device %s: %s\n",
	       SMR_DEVICE,
	       smr_info->dev_info->zbd_vendor_id);
	smr_log("    %s interface, %s disk model\n",
	       zbc_disk_type_str(smr_info->dev_info->zbd_type),
	       zbc_disk_model_str(smr_info->dev_info->zbd_model));
	smr_log("    %llu logical blocks of %u B\n",
	       (unsigned long long) smr_info->dev_info->zbd_logical_blocks,
	       (unsigned int) smr_info->dev_info->zbd_logical_block_size);
	smr_log("    %llu physical blocks of %u B\n",
	       (unsigned long long) smr_info->dev_info->zbd_physical_blocks,
	       (unsigned int) smr_info->dev_info->zbd_physical_block_size);
	smr_log("    %.03F GB capacity\n",
	       (double) (smr_info->dev_info->zbd_physical_blocks * 
			 smr_info->dev_info->zbd_physical_block_size) / 1000000000);

	smr_log("Target zone: Zone %d / %d, type 0x%x (%s), "
		"cond 0x%x (%s), need_reset %d, non_seq %d, "
		"LBA %llu, %llu sectors, wp %llu\n",
	       zoneidx,
	       smr_info->nr_zones,
	       zbc_zone_type(iozone),
	       zbc_zone_type_str(zbc_zone_type(iozone)),
	       zbc_zone_condition(iozone),
	       zbc_zone_condition_str(zbc_zone_condition(iozone)),
	       zbc_zone_need_reset(iozone),
	       zbc_zone_non_seq(iozone),
	       zbc_zone_start_lba(iozone),
	       zbc_zone_length(iozone),
	       zbc_zone_wp_lba(iozone));

#endif /* SMR_DEBUG */


	/* Check I/O size alignment. SMR zone write must be 4KB aligned */
	if ( zbc_zone_sequential_req(iozone) ) {
		ioalign = smr_info->dev_info->zbd_physical_block_size;
	} else {
		ioalign = smr_info->dev_info->zbd_logical_block_size;
	}
	
	if ( iosize % ioalign ) {
		smr_log("I/O size must be aligned\n");
		nbytes = -1;
		goto err_out;
	}


	/* Do not exceed the end of the zone */
	lba_count = iosize / smr_info->dev_info->zbd_logical_block_size;
	/*
	if ( zbc_zone_sequential(iozone) ) {
		if ( zbc_zone_full(iozone) ) {
			smr_log("iozone %d is full, write nothing\n", 
				*zoneidx);
			goto err_out;
		} else {
			lba_ofst = zbc_zone_wp_lba(iozone) - 
				zbc_zone_start_lba(iozone);
		}
	}
	*/
	/*
	if ( (lba_ofst + lba_count) > (long long)zbc_zone_length(iozone) ) {
		lba_count = zbc_zone_length(iozone) - lba_ofst;
	}
	*/

	if (!lba_count) {
		goto err_out;
	}

	total_lba = 0;
	bufofst = buf;
	lba_left = iosize / smr_info->dev_info->zbd_logical_block_size;
	lba_count = lba_left;
	bool write_to_next_zone = false;
	while (lba_left>0){
		zoneidx =  smr_lba_2_zone(start_lba);
		smr_log("czc: write to the zone: %d\n", zoneidx);
        iozone = &smr_info->zones[zoneidx];
        lba_ofst = start_lba %
            (SMR_ZONE_SIZE/smr_info->dev_info->zbd_logical_block_size);
        lba_max = zbc_zone_length(iozone) - lba_ofst;
		lba_count = lba_left;
		lba_count = lba_count > lba_max ? lba_max : lba_count;
		lba_left -=lba_count;
        start_lba +=lba_count;
		while (lba_count){
                /* write at most 256K one time */
            nlba = 256*1024 / smr_info->dev_info->zbd_logical_block_size;
			nlba = nlba > lba_count? lba_count: nlba;
			smr_log("smr_write:  zoneidx=%d, lba_ofst=%ld, lba_count=%ld, lba_max=%ld, nlba=%d\n",
            zoneidx, lba_ofst, lba_count, lba_max, nlba);
            nlba = zbc_pwrite(smr_info->dev, iozone,
                                  buf, nlba, lba_ofst);
			smr_log("write_pointer after call zbc_pwrite:%ld\n",zbc_zone_wp_lba(iozone));
			if ( nlba <= 0 )
				break;
			lba_count -= nlba;
			lba_ofst += nlba;
			total_lba += nlba;
			smr_log("the content: <%c>\n", bufofst[4]);
			bufofst += nlba * smr_info->dev_info->zbd_logical_block_size;
		}
	}
	/* convert #block into #Bytes */
	nbytes = total_lba * smr_info->dev_info->zbd_logical_block_size;

	/* update cnr_table */
	//smr_info->cnr_table[cnr_id] = start_lba;
	smr_log("czc: smr_write: update cnr_table[%ld]=%ld\n", cnr_id, start_lba);

#ifdef SMR_DEBUG
	smr_log("After write:\n");
	smr_log("Target zone: Zone %d / %d, type 0x%x (%s), "
		"cond 0x%x (%s), need_reset %d, non_seq %d, "
		"LBA %llu, %llu sectors, wp %llu\n",
	       zoneidx,
	       smr_info->nr_zones,
	       zbc_zone_type(iozone),
	       zbc_zone_type_str(zbc_zone_type(iozone)),
	       zbc_zone_condition(iozone),
	       zbc_zone_condition_str(zbc_zone_condition(iozone)),
	       zbc_zone_need_reset(iozone),
	       zbc_zone_non_seq(iozone),
	       zbc_zone_start_lba(iozone),
	       zbc_zone_length(iozone),
	       zbc_zone_wp_lba(iozone));
#endif /* SMR_DEBUG */

err_out:
	/* on failure */
	return nbytes;
}

size_t /* byte successfully read, -1 on error */
smr_lba_read(uint64_t lba, /*the lba to write the data*/
	 uint64_t cnr_id, /*container ID*/
	 char* buf, /* fixed sized buffer, space allocated by caller */
	 size_t iosize)
{
	int64_t lba_count, lba_ofst, total_lba, lba_max, lba_left, start_lba;
	int32_t nlba;
	char *bufofst;
	struct zbc_zone *iozone;
    struct smr_info* smr_info = GET_FPDD_DATA()->smr_info;
	int32_t zoneidx =  smr_lba_2_zone(lba);
	size_t nbytes;
	

	if (!buf)
		goto err_out;

	iozone = &smr_info->zones[zoneidx];
#ifdef SMR_DEBUG
	smr_log("smr_read: lba=%ld, zoneidx=%d\n", lba, zoneidx);
	smr_log("the location of wp before read: %ld\n",zbc_zone_wp_lba(iozone));

	smr_log("Device %s: %s\n",
		SMR_DEVICE,
	       smr_info->dev_info->zbd_vendor_id);
	smr_log("    %s interface, %s disk model\n",
	       zbc_disk_type_str(smr_info->dev_info->zbd_type),
	       zbc_disk_model_str(smr_info->dev_info->zbd_model));
	smr_log("    %llu logical blocks of %u B\n",
	       (unsigned long long) smr_info->dev_info->zbd_logical_blocks,
	       (unsigned int) smr_info->dev_info->zbd_logical_block_size);
	smr_log("    %llu physical blocks of %u B\n",
	       (unsigned long long) smr_info->dev_info->zbd_physical_blocks,
	       (unsigned int) smr_info->dev_info->zbd_physical_block_size);
	smr_log("    %.03F GB capacity\n",
	       (double) (smr_info->dev_info->zbd_physical_blocks * 
			 smr_info->dev_info->zbd_physical_block_size) /1000000000);

	smr_log("Target zone: Zone %d / %d, type 0x%x (%s), "
		"cond 0x%x (%s), need_reset %d, non_seq %d, "
		"LBA %llu, %llu sectors, wp %llu\n",
		zoneidx,
	       smr_info->nr_zones,
	       zbc_zone_type(iozone),
	       zbc_zone_type_str(zbc_zone_type(iozone)),
	       zbc_zone_condition(iozone),
	       zbc_zone_condition_str(zbc_zone_condition(iozone)),
	       zbc_zone_need_reset(iozone),
	       zbc_zone_non_seq(iozone),
	       zbc_zone_start_lba(iozone),
	       zbc_zone_length(iozone),
	       zbc_zone_wp_lba(iozone));
#endif /* SMR_DEBUG */

	/* Check alignment and get an I/O buffer */
	if ( iosize % smr_info->dev_info->zbd_logical_block_size ) {
		smr_log("Invalid I/O size %zu (must be aligned on %u)\n",
			iosize,
			(unsigned int) smr_info->dev_info->zbd_logical_block_size);
		goto err_out;
	}

	/*
	if ( zbc_zone_sequential_req(iozone)
	     && (! zbc_zone_full(iozone)) ) {
		lba_max = zbc_zone_wp_lba(iozone) - 
			zbc_zone_start_lba(iozone);
	} else {
		lba_max = zbc_zone_length(iozone);
	}

	if (cnr_id < 0 || cnr_id >= SMR_CNR_TABLE_SIZE) {
		smr_log("invalid cnr_id=%ld\n", cnr_id);
		goto err_out;
	}
	*/
	start_lba = lba;
	lba_ofst = lba % 
		(SMR_ZONE_SIZE/smr_info->dev_info->zbd_logical_block_size);
	lba_count = iosize / smr_info->dev_info->zbd_logical_block_size;
	lba_left = lba_count;
	total_lba = 0;
	bufofst = buf;
	while(lba_left>0){
		zoneidx =  smr_lba_2_zone(start_lba);
		iozone = &smr_info->zones[zoneidx];
		lba_ofst = start_lba %
        	(SMR_ZONE_SIZE/smr_info->dev_info->zbd_logical_block_size);
		lba_max = zbc_zone_length(iozone) - lba_ofst;
#ifdef SMR_DEBUG
		smr_log("smr_read:  zoneidx=%d, lba_ofst=%ld, lba_count=%ld, lba_max=%ld\n", 
			zoneidx, lba_ofst, lba_count, lba_max);
#endif
		lba_count = lba_left;
		lba_count = lba_count > lba_max ? lba_max : lba_count;
		
#ifdef SMR_DEBUG
		smr_log("smr_read:  lba_ofst=%ld, lba_count=%ld, lba_max=%ld\n", 
			lba_ofst, lba_count, lba_max);
#endif
		lba_left -=lba_count;
        start_lba +=lba_count;
		while(lba_count) {
		/* Read zone */
			nlba = 256*1024 / smr_info->dev_info->zbd_logical_block_size;
            nlba = nlba > lba_count? lba_count: nlba;
#ifdef SMR_DEBUG
			smr_log("smr_read:  zoneidx=%d, lba_ofst=%ld, lba_count=%ld, lba_max=%ld, nlba=%d\n",
            zoneidx, lba_ofst, lba_count, lba_max, nlba);
#endif
			nlba = zbc_pread(smr_info->dev, iozone, bufofst, 
				 	nlba, lba_ofst);
			if ( nlba <= 0 )
				break;
			lba_count -= nlba;
			lba_ofst += nlba;
			total_lba += nlba;
#ifdef SMR_DEBUG
			smr_log("the content: <%c>\n", bufofst[4]);
#endif
			bufofst += nlba * smr_info->dev_info->zbd_logical_block_size;
		}

		//smr_log("smr_read: after while, total_lba=%ld\n", total_lba);
	}

	/* convert #block into #Bytes */
    nbytes = total_lba * smr_info->dev_info->zbd_logical_block_size;
	//smr_log("smr_read: nbytes=%d\n", nbytes);
	return nbytes;

err_out:
	/* on failure */
	return -1;
}







/**************************************************************/

size_t /* bytes successfully written, -1 on error */
smr_write(uint64_t cnr_id, /*container ID*/
	  char *buf, 
	  size_t iosize,
	  int32_t stream_id,
	  int32_t *zoneidx) /* return value */
{
	struct zbc_zone *iozone;
	size_t ioalign;
	size_t nbytes;
	int64_t lba_count, lba_ofst, start_lba, total_lba;
	int32_t nlba;
	char* bufofst;

	struct smr_info* smr_info = GET_FPDD_DATA()->smr_info;
	if (!buf)
		goto err_out;

	*zoneidx = _smr_get_zone(smr_info, stream_id);

	smr_log("_smr_get_zone returns %d\n", *zoneidx);

	if (*zoneidx < 0){
		smr_log("cannot _smr_get_zone\n");
		nbytes = -1;
		goto err_out;
	}
	
        /* Get target zone */
	if ( *zoneidx >= (int)smr_info->nr_zones ) {
		smr_log("Target zone not found\n");
		nbytes = -1;
		goto err_out;
	}
	iozone = &smr_info->zones[*zoneidx];

	start_lba = zbc_zone_wp_lba(iozone);


#ifdef SMR_DEBUG
	smr_log("Device %s: %s\n",
	       SMR_DEVICE,
	       smr_info->dev_info->zbd_vendor_id);
	smr_log("    %s interface, %s disk model\n",
	       zbc_disk_type_str(smr_info->dev_info->zbd_type),
	       zbc_disk_model_str(smr_info->dev_info->zbd_model));
	smr_log("    %llu logical blocks of %u B\n",
	       (unsigned long long) smr_info->dev_info->zbd_logical_blocks,
	       (unsigned int) smr_info->dev_info->zbd_logical_block_size);
	smr_log("    %llu physical blocks of %u B\n",
	       (unsigned long long) smr_info->dev_info->zbd_physical_blocks,
	       (unsigned int) smr_info->dev_info->zbd_physical_block_size);
	smr_log("    %.03F GB capacity\n",
	       (double) (smr_info->dev_info->zbd_physical_blocks * 
			 smr_info->dev_info->zbd_physical_block_size) / 1000000000);

	smr_log("Target zone: Zone %d / %d, type 0x%x (%s), "
		"cond 0x%x (%s), need_reset %d, non_seq %d, "
		"LBA %llu, %llu sectors, wp %llu\n",
	       *zoneidx,
	       smr_info->nr_zones,
	       zbc_zone_type(iozone),
	       zbc_zone_type_str(zbc_zone_type(iozone)),
	       zbc_zone_condition(iozone),
	       zbc_zone_condition_str(zbc_zone_condition(iozone)),
	       zbc_zone_need_reset(iozone),
	       zbc_zone_non_seq(iozone),
	       zbc_zone_start_lba(iozone),
	       zbc_zone_length(iozone),
	       zbc_zone_wp_lba(iozone));

#endif /* SMR_DEBUG */


	/* Check I/O size alignment. SMR zone write must be 4KB aligned */
	if ( zbc_zone_sequential_req(iozone) ) {
		ioalign = smr_info->dev_info->zbd_physical_block_size;
	} else {
		ioalign = smr_info->dev_info->zbd_logical_block_size;
	}
	
	if ( iosize % ioalign ) {
		smr_log("I/O size must be aligned\n");
		nbytes = -1;
		goto err_out;
	}


	/* Do not exceed the end of the zone */
	lba_count = iosize / smr_info->dev_info->zbd_logical_block_size;
	if ( zbc_zone_sequential(iozone) ) {
		if ( zbc_zone_full(iozone) ) {
			smr_log("iozone %d is full, write nothing\n", 
				*zoneidx);
			goto err_out;
		} else {
			lba_ofst = zbc_zone_wp_lba(iozone) - 
				zbc_zone_start_lba(iozone);
		}
	}

	smr_log("smr_write: lba_ofst=%ld, lba_count=%ld\n", lba_ofst, lba_count);

	if ( (lba_ofst + lba_count) > (long long)zbc_zone_length(iozone) ) {
		lba_count = zbc_zone_length(iozone) - lba_ofst;
	}

	smr_log("smr_write: lba_ofst=%ld, lba_count=%ld\n", lba_ofst, lba_count);

	if (!lba_count) {
		goto err_out;
	}

	total_lba = 0;
	bufofst = buf;
	while (lba_count){
                /* write at most 256K one time */
                nlba = 256*1024 / smr_info->dev_info->zbd_logical_block_size;

		nlba = nlba > lba_count? lba_count: nlba;
                nlba = zbc_pwrite(smr_info->dev, iozone,
                                  buf, nlba, lba_ofst);
		if ( nlba <= 0 )
			break;
		lba_count -= nlba;
		lba_ofst += nlba;
		total_lba += nlba;
		bufofst += nlba * smr_info->dev_info->zbd_logical_block_size;
	}
	/* convert #block into #Bytes */
	nbytes = total_lba * smr_info->dev_info->zbd_logical_block_size;

	/* update cnr_table */
	smr_info->cnr_table[cnr_id] = start_lba;
	smr_log("smr_write: update cnr_table[%ld]=%ld\n", cnr_id, start_lba);

#ifdef SMR_DEBUG
	smr_log("After write:\n");
	smr_log("Target zone: Zone %d / %d, type 0x%x (%s), "
		"cond 0x%x (%s), need_reset %d, non_seq %d, "
		"LBA %llu, %llu sectors, wp %llu\n",
	       *zoneidx,
	       smr_info->nr_zones,
	       zbc_zone_type(iozone),
	       zbc_zone_type_str(zbc_zone_type(iozone)),
	       zbc_zone_condition(iozone),
	       zbc_zone_condition_str(zbc_zone_condition(iozone)),
	       zbc_zone_need_reset(iozone),
	       zbc_zone_non_seq(iozone),
	       zbc_zone_start_lba(iozone),
	       zbc_zone_length(iozone),
	       zbc_zone_wp_lba(iozone));
#endif /* SMR_DEBUG */

err_out:
	/* on failure */
	return nbytes;
}

size_t /* byte successfully read, -1 on error */
smr_read(uint64_t cnr_id, /*container ID*/
	 char* buf, /* fixed sized buffer, space allocated by caller */
	 size_t iosize)
{
	int64_t lba_count, lba_ofst, total_lba, lba_max;
	int32_t nlba;
	char *bufofst;
	struct zbc_zone *iozone;
        struct smr_info* smr_info = GET_FPDD_DATA()->smr_info;
	int32_t zoneidx = smr_get_zoneidx(cnr_id);
	size_t nbytes;
	
	smr_log("smr_read: cnr_id=%ld, zoneidx=%d\n", cnr_id, zoneidx);

	if (!buf)
		goto err_out;

	iozone = &smr_info->zones[zoneidx];

#ifdef SMR_DEBUG
	smr_log("Device %s: %s\n",
		SMR_DEVICE,
	       smr_info->dev_info->zbd_vendor_id);
	smr_log("    %s interface, %s disk model\n",
	       zbc_disk_type_str(smr_info->dev_info->zbd_type),
	       zbc_disk_model_str(smr_info->dev_info->zbd_model));
	smr_log("    %llu logical blocks of %u B\n",
	       (unsigned long long) smr_info->dev_info->zbd_logical_blocks,
	       (unsigned int) smr_info->dev_info->zbd_logical_block_size);
	smr_log("    %llu physical blocks of %u B\n",
	       (unsigned long long) smr_info->dev_info->zbd_physical_blocks,
	       (unsigned int) smr_info->dev_info->zbd_physical_block_size);
	smr_log("    %.03F GB capacity\n",
	       (double) (smr_info->dev_info->zbd_physical_blocks * 
			 smr_info->dev_info->zbd_physical_block_size) /1000000000);

	smr_log("Target zone: Zone %d / %d, type 0x%x (%s), "
		"cond 0x%x (%s), need_reset %d, non_seq %d, "
		"LBA %llu, %llu sectors, wp %llu\n",
		zoneidx,
	       smr_info->nr_zones,
	       zbc_zone_type(iozone),
	       zbc_zone_type_str(zbc_zone_type(iozone)),
	       zbc_zone_condition(iozone),
	       zbc_zone_condition_str(zbc_zone_condition(iozone)),
	       zbc_zone_need_reset(iozone),
	       zbc_zone_non_seq(iozone),
	       zbc_zone_start_lba(iozone),
	       zbc_zone_length(iozone),
	       zbc_zone_wp_lba(iozone));
#endif /* SMR_DEBUG */

	/* Check alignment and get an I/O buffer */
	if ( iosize % smr_info->dev_info->zbd_logical_block_size ) {
		smr_log("Invalid I/O size %zu (must be aligned on %u)\n",
			iosize,
			(unsigned int) smr_info->dev_info->zbd_logical_block_size);
		goto err_out;
	}


	if ( zbc_zone_sequential_req(iozone)
	     && (! zbc_zone_full(iozone)) ) {
		lba_max = zbc_zone_wp_lba(iozone) - 
			zbc_zone_start_lba(iozone);
	} else {
		lba_max = zbc_zone_length(iozone);
	}

	if (cnr_id < 0 || cnr_id >= SMR_CNR_TABLE_SIZE) {
		smr_log("invalid cnr_id=%ld\n", cnr_id);
		goto err_out;
	}
	lba_ofst = smr_info->cnr_table[cnr_id] % 
		(SMR_ZONE_SIZE/smr_info->dev_info->zbd_logical_block_size);
	lba_count = iosize / smr_info->dev_info->zbd_logical_block_size;

	if ( zbc_zone_sequential_req(iozone)
	     && (! zbc_zone_full(iozone)) ) {
		lba_max = zbc_zone_wp_lba(iozone) - 
			zbc_zone_start_lba(iozone) - 
			lba_ofst;
	} else {
		lba_max = zbc_zone_length(iozone) - lba_ofst;
	}

	smr_log("smr_read:  lba_ofst=%ld, lba_count=%ld, lba_max=%ld\n", 
		lba_ofst, lba_count, lba_max);

	lba_count = iosize / smr_info->dev_info->zbd_logical_block_size;
	lba_count = lba_count > lba_max ? lba_max : lba_count;

	smr_log("smr_read:  lba_ofst=%ld, lba_count=%ld, lba_max=%ld\n", 
		lba_ofst, lba_count, lba_max);

	total_lba = 0;
	bufofst = buf;
	while(lba_count) {
		/* Read zone */
		nlba = 256*1024 / smr_info->dev_info->zbd_logical_block_size;
                nlba = nlba > lba_count? lba_count: nlba;
		nlba = zbc_pread(smr_info->dev, iozone, bufofst, 
				 nlba, lba_ofst);
		if ( nlba <= 0 )
			break;
		lba_count -= nlba;
		lba_ofst += nlba;
		total_lba += nlba;
		bufofst += nlba * smr_info->dev_info->zbd_logical_block_size;
	}

	smr_log("smr_read: after while, total_lba=%ld\n", total_lba);

	/* convert #block into #Bytes */
        nbytes = total_lba * smr_info->dev_info->zbd_logical_block_size;
	smr_log("smr_read: nbytes=%d\n", nbytes);
	return nbytes;

err_out:
	/* on failure */
	return -1;
}


int32_t  /* zoneidx, -1 on error */
smr_get_zoneidx(int64_t cnr_id)
{
	struct smr_info* smr_info = GET_FPDD_DATA()->smr_info;
	int32_t zoneidx;
	if (cnr_id < 0 || cnr_id >= SMR_CNR_TABLE_SIZE) {
		smr_log("invalid cnr_id=%ld\n", cnr_id);
		return -1;
	}

	if (smr_info->cnr_table[cnr_id] < 0) {
		smr_log("cnr_table[%ld] is does not exist yet\n", cnr_id);
		return -1;
	}

	zoneidx = smr_info->cnr_table[cnr_id] / 
		(SMR_ZONE_SIZE/smr_info->dev_info->zbd_logical_block_size);
	smr_log("smr_get_zoneidx: cnr_id=%ld lba=%ld zoneidx=%d\n", 
		cnr_id, smr_info->cnr_table[cnr_id], zoneidx);
	
	return zoneidx;
}

/* call this funtion when  removing a container */
int smr_invalidate_cnr(int64_t cnr_id)
{
	struct smr_info* smr_info = GET_FPDD_DATA()->smr_info;
	if (!smr_info) {
		smr_log("smr_info not initialized!\n");
		return -1;
	}
	smr_info->cnr_table[cnr_id] = -1;
	return 0;
}

int smr_lba_reset_pointer(int64_t lba)
{
	int64_t wp_lba;
}

int smr_reset_pointer(int32_t zoneidx)
{
	smr_log("smr_reset_pointer: %d\n", zoneidx);
	int ret;
	struct zbc_zone *rzone = NULL;
	uint64_t lba;
	struct smr_info* smr_info = GET_FPDD_DATA()->smr_info;

	rzone = &smr_info->zones[zoneidx];

	smr_log("Device %s: %s\n",
	       SMR_DEVICE,
	       smr_info->dev_info->zbd_vendor_id);
	smr_log("    %s interface, %s disk model\n",
	       zbc_disk_type_str(smr_info->dev_info->zbd_type),
	       zbc_disk_model_str(smr_info->dev_info->zbd_model));
	smr_log("    %llu logical blocks of %u B\n",
	       (unsigned long long) smr_info->dev_info->zbd_logical_blocks,
	       (unsigned int) smr_info->dev_info->zbd_logical_block_size);
	smr_log("    %llu physical blocks of %u B\n",
	       (unsigned long long) smr_info->dev_info->zbd_physical_blocks,
	       (unsigned int) smr_info->dev_info->zbd_physical_block_size);
	smr_log("    %.03F GB capacity\n",
	       (double) (smr_info->dev_info->zbd_physical_blocks * 
			 smr_info->dev_info->zbd_physical_block_size) /	
	       1000000000);

	smr_log("Target zone: Zone %d / %d, type 0x%x (%s), "
		"cond 0x%x (%s), need_reset %d, non_seq %d, "
		"LBA %llu, %llu sectors, wp %llu\n",
		zoneidx,
	       smr_info->nr_zones,
	       zbc_zone_type(rzone),
	       zbc_zone_type_str(zbc_zone_type(rzone)),
	       zbc_zone_condition(rzone),
	       zbc_zone_condition_str(zbc_zone_condition(rzone)),
	       zbc_zone_need_reset(rzone),
	       zbc_zone_non_seq(rzone),
	       zbc_zone_start_lba(rzone),
	       zbc_zone_length(rzone),
	       zbc_zone_wp_lba(rzone));

	lba = zoneidx * SMR_LBA_PER_ZONE;

	/* Reset WP */
	smr_log("reset pointer at lba=:%ld\n",lba);
	ret = zbc_reset_write_pointer(smr_info->dev, lba);

	if ( ret != 0 ) {
		smr_log("zbc_reset_write_pointer failed zone/lba=%d/%lld\n",
			zoneidx, lba);
		ret = 1;
	}

	smr_log("Target zone: Zone %d / %d, type 0x%x (%s), "
		"cond 0x%x (%s), need_reset %d, non_seq %d, "
		"LBA %llu, %llu sectors, wp %llu\n",
		zoneidx,
	       smr_info->nr_zones,
	       zbc_zone_type(rzone),
	       zbc_zone_type_str(zbc_zone_type(rzone)),
	       zbc_zone_condition(rzone),
	       zbc_zone_condition_str(zbc_zone_condition(rzone)),
	       zbc_zone_need_reset(rzone),
	       zbc_zone_non_seq(rzone),
	       zbc_zone_start_lba(rzone),
	       zbc_zone_length(rzone),
	       zbc_zone_wp_lba(rzone));
	
	smr_info->zone2stream[zoneidx] = -1;
	return ret;
}

/*
 * get all the full zones in SMR.
 * @param full_zone_array, each item in full_zone_array will be the full zone
 *         index
 * @param full_zone_array, the number or full zones. The first
 *         *full_zone_array* items is valid in the full_zone_array
 */
int smr_get_full_zones(int32_t *full_zone_array, int32_t *full_zone_cnt)
{
        int32_t i;
	struct smr_info* smr_info = GET_FPDD_DATA()->smr_info;

	*full_zone_cnt = 0;
	for (i = 64; i < (int32_t)smr_info->nr_zones; i++) {
		if(zbc_zone_full(&smr_info->zones[i]))
			full_zone_array[(*full_zone_cnt)++] = i;
	}
}

void smr_selftest()
{

#define SMR_TEST_BUFSIZE (128*1024*1024)
	smr_log("enter smr_selftest\n");
	char *buf, *buf_read;
	size_t nbytes;
	uint64_t cnr_id = 5;
	int32_t stream_id = 9;
	int32_t zoneidx = -1;
	int i, remain, memset_size, ret;

	buf = malloc(SMR_TEST_BUFSIZE * sizeof(char));
	if (!buf) {
		smr_log("unable malloc for buf\n");
		return;
	}
	
	smr_log("init buffers \n");

	remain = SMR_TEST_BUFSIZE;
	i = 0;
	while (remain) {
		memset_size = 256 * 1024;
		memset_size = remain > memset_size? memset_size: remain;
		memset(buf + i, 'w', memset_size);
		remain -= memset_size;
		i += memset_size;
	}

	buf_read = malloc(SMR_TEST_BUFSIZE * sizeof(char));
	if (!buf_read) {
		smr_log("unable malloc for buf_read\n");
		return;
	}

	remain = SMR_TEST_BUFSIZE;
	i = 0;
	while (remain) {
		memset_size = 256 * 1024;
		memset_size = remain > memset_size? memset_size: remain;
		memset(buf_read + i, '-', memset_size);
		remain -= memset_size;
		i += memset_size;
	}

	smr_log("smr_write check <%c>\n", buf[4]);
	smr_log("smr_write check <%c>\n", buf[SMR_TEST_BUFSIZE - 4]);
	nbytes = smr_write(cnr_id, buf, SMR_TEST_BUFSIZE, 
			   stream_id, &zoneidx);
	smr_log("smr_write returns %d\n", nbytes);

	nbytes = smr_read(cnr_id, buf_read, SMR_TEST_BUFSIZE);
	smr_log("smr_read  returns %d\n", nbytes);

	smr_log("smr_read check <%c>\n", buf_read[4]);
	smr_log("smr_read check <%c>\n", buf_read[SMR_TEST_BUFSIZE - 4]);
	
/*
	smr_log("smr_reset_pointer zoneidx=%d\n", zoneidx);
	ret = smr_reset_pointer(zoneidx);
	smr_log("smr_reset_pointer zoneidx=%d, returns %d\n", zoneidx, ret);
*/
	smr_log("exit smr_selftest\n");
}




