/*
 * storage-manager.h
 *
 *  Created on: 2012-7-14
 *      Author: BadBoy
 */

#ifndef STORAGE_MANAGER_H_
#define STORAGE_MANAGER_H_



struct storage_manager
{
	FILE *f;
	uint64_t allocted_offset;
};

uint64_t get_new_block(struct storage_manager * manager, uint64_t len);

uint64_t get_new_seg(struct storage_manager * manager);

int storage_manager_init(struct storage_manager * manager, char * file_name);
#endif /* STORAGE_MANAGER_H_ */
