/*
 * adaptive_faa.h
 *
 *  Created on: 03/18/2017
 *      Author: zhichao cao
 */

#ifndef ADAPTIVE_FAA_H_
#define ADAPTIVE_FAA_H_

struct _faa_buf
{
	uint64_t  assemble_count;
	int filled_num;
	char *buf;
	struct _faa_buf *next;
	struct _faa_buf *pre;
};

struct _faa
{
	int num;
	uint64_t head_count;
	uint64_t tail_count;
	struct _faa_buf buf_head;
};

struct _adaptive_faa_parm
{
	int container_reads;  //container reads in this faa window restore
	int hit;				//cache hit number
	int miss;				//cache miss number should be the same as container reads
	int e_add;				//chunks added to the e cache
	int p_add;				//chunks added to the p cache
	int e2p;				//moved from ecache to pcache
	int p2e;				//moved from pcache to ecache
	int e_num;				//ecache chunuk number
	int p_num;				//pcache chunk number
	int effective_faa;		//accumulate the consecutive container reads lower than threshold, if larger than total FAA, add more
	int effective_cache;	//consecutive container reads larger than threshold, benefit from cache;
	int faa_change;			//how to adajust, increase or decrease 1
	int law_change;			//look ahead window adjustment, increase a lot or decrease 1;
	int last_faa;			//the latest faa size used, in container number
	int last_cache;			//the last cache size used, in conatiner number
	int last_law;			//the last look ahead window size, in contianers number
};

struct _faa *faa_init(void);

void faa_destroy(struct _faa *faa);

struct _faa_buf *faa_buf_init(uint64_t assemble_count);

void faa_buf_destroy(struct _faa_buf *tmp);

void faa_buf_remove(struct _faa_buf *tmp);

void faa_add_tail(struct _faa_buf *tmp);

struct _faa_buf * faa_remove_head(void);

void adaptive_faa_adjust(struct _adaptive_faa_parm *faa_parm);

int restore_assemble_adaptive_faa(uint64_t assemble_count, FILE *fp, struct _adaptive_faa_parm *faa_parm);


#endif
