/*
 * data.h
 *
 *  Created on: 2012-7-13
 *      Author: BadBoy
 */

#ifndef DATA_H_
#define DATA_H_


struct data_seg_header
{
	uint64_t previous;
	uint64_t next;
};

#define DATA_PER_SEG (SEG_SIZE - sizeof(struct data_seg_header))

struct data_seg
{
	struct data_seg_header header;
	uint64_t data_seg_offset;
	uint32_t len;
	struct storage_manager * manager;
	char data[DATA_PER_SEG];
};

int data_init(struct data_seg * dt_seg);
#ifdef CHUNK_MODE
uint64_t add_data(char *buf, uint32_t len, struct data_seg * dt_seg, uint64_t counter);
int get_data(char *buf, uint32_t len, uint64_t counter);
#else
uint64_t add_data(char *buf, uint32_t len, struct data_seg * dt_seg);
int get_data(char *buf, uint64_t offset, uint32_t len, struct data_seg * dt_seg);
#endif




#endif /* DATA_H_ */
