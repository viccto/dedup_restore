/* 
 * direct_rw.h, it is used simulate the directly read and write to the deduplicated files
 * authoer: zhichao cao
 * created date: 03/25/2017
 */

#ifndef _DIRECT_RW_H_
#define  _DIRECT_RW_H_

int direct_rw_sequential_test(char * file_name);

int direct_rw_random_test(char * file_name);

int read_from_reloaded(char *buf, uint64_t size, uint64_t offset, uint64_t chunk_size, char * file_name);

int write_to_reloaded(char *buf, uint64_t size, uint64_t offset, uint64_t chunk_size, char * file_name);

#endif
