/*
 ============================================================================
 Name        : zone_allocation.c
 Author      : Zhichao Cao
 Date        : 12/02/2016
 Copyright   : Your copyright notice
 Description : do the statistic for the zones when dedup the file and decide the zone number of the current container
 ============================================================================
 */


#include <fcntl.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <time.h> 

#include "enums.h"
#include "config.h"
#include "storagemanager.h"
#include "file.h"
#include "disk.h"
#include "diskhash.h"
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
#include "smr.h"
#include "zone_allocation.h"

struct _zone_stat *zone_allocation_init(void)
{
	struct _zone_stat *zone_stat;
	zone_stat = (struct _zone_stat *)malloc(sizeof(struct _zone_stat));
	clean_zone_stat(zone_stat);
	return zone_stat;
}

int clean_zone_stat(struct _zone_stat *zone_stat)
{
	uint64_t i;
	zone_stat->current_head = 0;
	zone_stat->max_pos = 0;
	zone_stat->in_use = false;
	for(i=0; i<MAX_ZONE_NUM; i++){
		zone_stat->cover_array[i] = 0;
	}
	return 1;
}

int zone_allocation_destroy(struct _zone_stat *zone_stat)
{
	free(zone_stat);
	return 1;
}	

int update_zone_cover(uint64_t new_pos)
{
	struct _zone_stat *zone_stat;
	zone_stat = GET_FPDD_DATA()->zone_stat;
	zone_stat->in_use = true;
	uint64_t i;
	for(i=zone_stat->current_head; i<new_pos;i++)
	{
		zone_stat->cover_array[i]++;
	}
	zone_stat->current_head = new_pos;
	if(new_pos>zone_stat->max_pos){
		zone_stat->max_pos = new_pos;
	}
	
	return 1;
}

/*
int64_t select_zone_allocate(void)
{
	struct _zone_stat *zone_stat;
	zone_stat = GET_FPDD_DATA()->zone_stat;
	uint64_t i, j max;
	max = 0;
	for(j=0;j<MAX_ALLOCATE_TRY;i++){
		for(i=0;i<=zone_stat->max_pos;i++){
			
		
*/
















