/*
 * zone_allocation.h
 *
 *  Created on: 2016-12-02
 *      Author: zhichao cao
 */

#ifndef ZONE_ALLOCATION_H_
#define ZONE_ALLOCATION_H_

struct _zone_stat
{
	uint64_t cover_array[MAX_ZONE_NUM];
	uint64_t current_head;
	uint64_t max_pos;
	bool in_use;
};




#endif
