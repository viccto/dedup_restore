/*
 * bloom-filter.h
 *
 *  Created on: 2012-5-31
 *      Author: badboy
 */

#ifndef BLOOM_FILTER_H_
#define BLOOM_FILTER_H_


typedef unsigned int (*hashfunc_t)(const char *);

struct _bloom {
    size_t asize;
    unsigned char *a;
    size_t nfuncs;
    hashfunc_t *funcs;
};

struct _bloom *bloom_create(size_t size, size_t nfuncs, ...);

struct _bloom * bloom_init(void);

void bloom_reload(struct _bloom * bloom);

void _bloom_reload(struct _bloom * bloom, size_t nfuncs, ...);

void bloom_destroy(struct _bloom *bloom);

int bloom_add(struct _bloom *bloom, const char *s);

int bloom_check(struct _bloom *bloom, const char *s);


unsigned int hash_func0(unsigned int *finger_prints);

unsigned int hash_func1(unsigned int *finger_prints);

unsigned int hash_func2(unsigned int  *finger_prints);

unsigned int hash_func3(unsigned int  *finger_prints);

unsigned int hash_func4(unsigned int  *finger_prints);

unsigned int hash_func5(unsigned int *finger_prints);

int set_bit(int nr,int * addr);

int test_bit(int nr, int * addr);

int bloom_filter_lookup(void * bf, unsigned int * element);

int bloomfilter_init(char * bf);


#endif /* BLOOM_FILTER_H_ */
