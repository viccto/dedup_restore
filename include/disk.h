/*
 * disk.c
 *
 *  Created on: 2012-5-22
 *      Author: badboy
 */

#ifndef DISK_H_
#define DISK_H_

void myseek(FILE * handle, uint64_t a);

int simpleread(uint64_t a, void *buf, size_t len, FILE * handle);

int simplewrite(uint64_t a, void *buf, size_t len, FILE * handle);
#endif
