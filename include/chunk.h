/*
 * chunk.c
 *
 *  Created on: 2012-7-13
 *      Author: BadBoy
 */
#ifndef CHUNK_H_
#define CHUNK_H_

int newly_chunk(char * chunk_buf, char * data, size_t len, int cont, int file_end, struct dedup_manager * de_dup, FILE *fp);

#endif
