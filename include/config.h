/*
 * config.h
 *
 *  Created on: 2017-2-03
 *      Author: Zhichao Cao
 */

#ifndef CONFIG_H_
#define CONFIG_H_

#define DIR_MODE (S_IRWXU | S_IRWXG | S_IRWXO)

#define  dedupedchunkdirectory "./dedupedchunk/"

#define  recipedirectory "./recipe/"

#define  restoredirectory "./restore/"

#define memstoredirectory "./memstore/"

#define METAFILE "./metadata"



/*define the dedup high water mark, when hit it, start dedup*/
#define H_WM (100)

/*define the dedup low water mark, when hit it, stop dedup*/
#define L_WM (600)

/*define the dedup file threshold, files' size larger than it can be deduped*/
#define DE_WM (500)

/*define the chunk numbers in one container*/
#define CHUNK_NUM 100

/*the max of one container, used in initiate the container in memory*/
#define MAX_CONTAINER_SIZE (4*1024*1024)

#define SEG_SIZE (4 * 1024)

#define FINGERPRINT_LEN 17

#define CACHE_MASK 0x7ff
#define CACHE_LEN 2048
//#define CACHE_BUCKET_LEN 256
#define CACHE_BUCKET_LEN 10

#define MEM_HASH_NUM 100
#define BUKET_NUM (64*1024)
#define DISK_HASH_MASK 0x3ff

#define CHUNK_MASK 0x03FF
#define MAX_CHUNK_LEN (16 * 1024)
#define MIN_CHUNK_LEN 512
#define WIN_LEN 48

#define BF_LEN (100*1024*1024)

#define TO_CACHE_LEN 1000

/*it is just used to debug in local*/

/* for the debug write log use */
//#define SMR_DEBUG
//#define RW_DEBUG
//#define DEDUP_DEBUG
//#define CONTAINER_DEBUG
//define TIME_DEBUG
//#define LOOK_AHEAD_DEBUG
//#define MEMSTORE_DEBUG
#define RESULT_PRINT

#define END 0
#define START 1
#define ALL 2

#define COMPRESS

#ifdef COMPRESS
	#define MAX_COMPRESS_LEN (2 * MAX_CHUNK_LEN)
#endif

/*******write cache eabled for the dedup table**********/
#define WRITE_CACHE
#define MAX_ZONE_NUM 29000
#define MAX_ALLOCATE_TRY 10

/*it is just used to debug in local*/
#define ZONE_SIZE_FAKE 256*1024*1024
#define ZONE_NUM_FAKE 29000
//#define SMR

/*****define the look ahead or one by one*************/
#define CACHE_CON 30  /*used for cache size ,how many container num*/
#define LOOK_AHEAD
#ifdef LOOK_AHEAD
	#define ASSEMBLE_BUF_SIZE 1024
	#define AHEAD_NUM_LOW_LIMIT 5
	#define AHEAD_NUM_HIGH_LIMIT 11
	#define LOOK_AHEAD_TIMES 32
	#define ASSEMBLY_BUFFER_INIT (32-CACHE_CON)
	#define LOOK_AHEAD_SIZE (ASSEMBLE_BUF_SIZE*LOOK_AHEAD_TIMES)
	//#define REGULAR_ASSEMBLE
	#define LOOK_AHEAD_ASSEMBLE
#endif

/*************************************************/


/**************define the container cache*******************/
#ifdef LOOK_AHEAD_ASSEMBLE
	//#define CONTAINER_CACHE
	//#define CHUNK_CACHE
#endif
#define CONTAINER_CACHE_BUCK 5
#define CONTAINER_CACHE_NUM (CACHE_CON*1)
#define CHUNK_INFO_HASH_NUM (512*CACHE_CON)

#define CHUNK_CACHE_BUCK (512*CACHE_CON)
//#define CHUNK_CACHE_NUM (1024*CACHE_CON)

/********************************************************/
/*in this model, the file always read the same container file, and always 
 * to the begining of the restored file, so the read and write operation
 * is real but we do not need much data*/
#define CONTENT_FREE_RESTORE   /*restore from the 0 container and always overwrite*/
#define CONTAINER_FREE_DEDUP   /*write no containers to the disk when dedup the file*/
#define CONTAINER_IO_DELAY 1 /*to simulate the container IO time for a 4MB container*/


/***********************************/


/*stream model*/
#define STREAM_ID_LIMIT 10
#define NEAR 1
#define RANDOM 0
#define CYCLE 0
/****************/


/*******memstore parameters************/
#define MEMSTORE
#define MEMSTORE_SIMULATE
#define MEMSTORE_NODE_NUM 512
#define MEMSTORE_NODE_BUCK 512
#define MEMSTORE_SEG_NUM 128
#define MEMSTORE_SEG_BUCK 2
#define MEMSTORE_NODE_CACHE_BUCK 512
#define MEMSTORE_NODE_CACHE_NUM 1024*64
#define MEMSTORE_BF_CACHE_NUM 128
#ifdef MEMSTORE
	#define BF_LEN (16*1024*1024)
#else
	#define BF_LEN (256*1024*1024)
#endif

#endif
