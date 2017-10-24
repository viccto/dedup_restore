#include <fcntl.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <dirent.h>
#include <string.h>

#include "enums.h"
#include "config.h"


void myseek(FILE * handle, uint64_t a)
{
	int ret;
	if ((ret = fseek(handle, a, SEEK_SET)) < 0)
	{
		//printf("seek locate %ld\n", a);
		//perror("Can not seek locally!");
	}
}

int simpleread(uint64_t a, void *buf, size_t len, FILE * handle)
{
	size_t res;
	//printf("read offset %ld\n", a);
	myseek(handle, a);
	res = fread(buf, 1, len, handle);
	return (res < 0 || (size_t)res != len);
}

int simplewrite(uint64_t a, void *buf, size_t len, FILE * handle)
{
	size_t res;
	//printf("write offset %ld\n", a);
	myseek(handle, a);
	res = fwrite(buf, 1, len, handle);
	return (res < 0 ||(size_t)res != len);
}

