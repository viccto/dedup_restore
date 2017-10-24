/*
 * file.h
 *
 *  Created on: 2012-7-12
 *      Author: BadBoy
 */

#ifndef FILE_H_
#define FILE_H_


struct file_seg_header
{
	uint64_t previous;
	uint64_t next;
	uint32_t file_num;
};

struct file
{
	char name[100];
	uint64_t metadata_start_offset;
	uint64_t metadata_end_offset;
	uint32_t chunk_num;
};

#define FILE_PER_SEG ((SEG_SIZE - sizeof(struct file_seg_header)) / sizeof(struct file))

struct file_seg
{
	uint64_t seg_offset;
	uint64_t start_seg_offset;
	uint32_t len;
	struct storage_manager * manager;
	struct file_seg_header header;
	struct file file[FILE_PER_SEG];
};

int file_init(struct file_seg * f_seg);

int add_2_file(struct file f, struct file_seg * f_seg);

int get_files(struct file * f, int len, int pos, struct file_seg * f_seg);

int get_file_by_name(struct file * f, struct file_seg * f_seg, char *name);



#endif /* FILE_H_ */
