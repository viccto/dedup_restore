#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <dirent.h>
#include <string.h>

#include "enums.h"
#include "config.h"
#include "storagemanager.h"

uint64_t get_new_block(struct storage_manager * manager, uint64_t len)
{
	uint64_t offset;
	offset = manager->allocted_offset;
	manager->allocted_offset += len;
	//printf("%ld\n", offset);
	return offset;
}

uint64_t get_new_seg(struct storage_manager * manager)
{
	return get_new_block(manager, SEG_SIZE);
}

int storage_manager_init(struct storage_manager * manager, char * file_name)
{
	manager->f= fopen(file_name, "w+");
	if(NULL == manager->f)
	{
		return -1;
	}
	manager->allocted_offset = 0;
	return 0;
}

