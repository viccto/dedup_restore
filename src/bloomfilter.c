#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include<limits.h>
#include<stdarg.h>

#include "enums.h"
#include "config.h"
#include "bloomfilter.h"


unsigned int sax_hash(const char *key)
{
    unsigned int h=0;

    while(*key) h^=(h<<5)+(h>>2)+(unsigned char)*key++;

    return h;
}

unsigned int sdbm_hash(const char *key)
{
    unsigned int h=0;
    while(*key) h=(unsigned char)*key++ + (h<<6) + (h<<16) - h;
    return h;
}

unsigned int hash_func0(unsigned int *finger_prints)
{
	unsigned int position = 0;
	position =  *(finger_prints + 4) & 0x3ffffff;
	return position;
}

unsigned int hash_func1(unsigned int *finger_prints)
{
	unsigned int position = 0;
	position =  ((*(finger_prints + 4)) >> 26) | (((*(finger_prints + 3)) & 0xfffff) << 6);
	return position;
}

unsigned int hash_func2(unsigned int  *finger_prints)
{
	unsigned int position = 0;
	position = ((*(finger_prints + 3)) >> 20) | ((*(finger_prints + 2) & 0x7fff) << 12);
	return position;
}

unsigned int hash_func3(unsigned int  *finger_prints)
{
	unsigned int position = 0;
	position = ((*(finger_prints + 2)) >> 15) | ((*(finger_prints + 1) & 0x3ff) << 17);
	return position;
}

unsigned int hash_func4(unsigned int  *finger_prints)
{
	unsigned int position = 0;
	position = ((*(finger_prints + 1)) >> 10) | ((*(finger_prints) & 0x1f) << 22);
	return position;
}

unsigned int hash_func5(unsigned int *finger_prints)
{
	unsigned int position = 0;
	position = (*(finger_prints)) >> 5;
	return position;
}


int set_bit(int nr,int * addr)
{
        int     mask, retval;

        addr += nr >> 5;
        mask = 1 << (nr & 0x1f);
        retval = (mask & *addr) != 0;
        *addr |= mask;
        return retval;
}
 int test_bit(int nr, int * addr)
{
        int     mask;

        addr += nr >> 5;
        mask = 1 << (nr & 0x1f);
        return ((mask & *addr) != 0);
}


#define SETBIT(a, n) (a[n/CHAR_BIT] |= (1<<(n%CHAR_BIT)))
#define GETBIT(a, n) (a[n/CHAR_BIT] & (1<<(n%CHAR_BIT)))

struct _bloom *bloom_create(size_t size, size_t nfuncs, ...)
{
    struct _bloom *bloom;
    va_list l;
    int n;
    
    if(!(bloom=malloc(sizeof(struct _bloom)))) 
		return NULL;
    if(!(bloom->a=calloc((size+CHAR_BIT-1)/CHAR_BIT, sizeof(char)))) 
	{
        free(bloom);
        return NULL;
    }
    if(!(bloom->funcs=(hashfunc_t*)malloc(nfuncs*sizeof(hashfunc_t)))) {
        free(bloom->a);
        free(bloom);
        return NULL;
    }

    va_start(l, nfuncs);
    for(n=0; n<nfuncs; ++n) {
        bloom->funcs[n]=va_arg(l, hashfunc_t);
    }
    va_end(l);

    bloom->nfuncs=nfuncs;
    bloom->asize=size;

    return bloom;
}

struct _bloom * bloom_init(void)
{
	struct _bloom * bloom;
	bloom = bloom_create(BF_LEN, 2, sax_hash, sdbm_hash);
	return bloom;
}

void bloom_reload(struct _bloom * bloom)
{
	_bloom_reload(bloom, 2, sax_hash, sdbm_hash);
	return;
}

void _bloom_reload(struct _bloom * bloom, size_t nfuncs, ...)
{
	va_list l;
	int n;

	if(!(bloom->funcs=(hashfunc_t*)malloc(nfuncs*sizeof(hashfunc_t)))) {
        free(bloom->a);
        free(bloom);
        return;
    }

    va_start(l, nfuncs);
    for(n=0; n<nfuncs; ++n) {
        bloom->funcs[n]=va_arg(l, hashfunc_t);
    }
    va_end(l);

    bloom->nfuncs=nfuncs;
	bloom->asize=BF_LEN;
	return;
}

void bloom_destroy(struct _bloom *bloom)
{
    free(bloom->a);
    free(bloom->funcs);
    free(bloom);

    return;
}

int bloom_add(struct _bloom *bloom, const char *s)
{
    size_t n;

    for(n=0; n<bloom->nfuncs; ++n) {
        SETBIT(bloom->a, bloom->funcs[n](s)%bloom->asize);
    }

    return 0;
}

int bloom_check(struct _bloom *bloom, const char *s)
{
    size_t n;

    for(n=0; n<bloom->nfuncs; ++n) {
        if(!(GETBIT(bloom->a, bloom->funcs[n](s)%bloom->asize))) return 0;
    }

    return 1;
}






/***********************************************************************
* function name = bloomfilter_lookup
* function = lookup the abstract in the bf and update the bf line
* input :
*         @bfset               the bloom filter bit set
*	    @element          the data abstract from the sha1
*
* return:
*          ret = 1                  find
*          ret = 0 		     not find
***********************************************************************/
int bloom_filter_lookup(void * bf, unsigned int * element)
{
	int ret = 1;
	if(test_bit(hash_func0(element),bf) &&
	  	test_bit(hash_func1(element),bf+ (1 << 23))&&
	  	test_bit(hash_func2(element),bf+ (1 << 24)) &&
	  	test_bit(hash_func3(element),bf+ (1 << 25)) &&
	  	test_bit(hash_func4(element),bf+ (1 << 25) + (1 << 24)) &&
	  	test_bit(hash_func5(element),bf+ (1 << 26)) ) {
		return ret;
   	}

	ret = 0;
	set_bit(hash_func0(element),bf);
  	set_bit(hash_func1(element),bf + (1 << 23));
  	set_bit(hash_func2(element),bf + (1 << 24));
  	set_bit(hash_func3(element),bf + (1 << 25));
  	set_bit(hash_func4(element),bf + (1 << 25) + (1 << 24));
  	set_bit(hash_func5(element),bf + (1 << 26));

	return ret;
}

int bloomfilter_init(char * bf)
{
	memset(bf, 0, BF_LEN);
	return 0;
}

