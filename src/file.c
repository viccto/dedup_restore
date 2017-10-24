#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <stdarg.h>

#include "enums.h"
#include "config.h"
#include "storagemanager.h"
#include "file.h"
#include "disk.h"
#include "optsmr.h"


int file_init(struct file_seg * f_seg)
{
	f_seg->len = 0;
	f_seg->seg_offset = get_new_seg(f_seg->manager);
	f_seg->start_seg_offset = f_seg->seg_offset;
	f_seg->header.previous = 0xFFFFFFFFFFFFFFFF;
	return 0;
}

int add_2_file(struct file f, struct file_seg * f_seg)
{
	uint64_t write_offset;
	if(f_seg->len >= FILE_PER_SEG)
	{
		write_offset = f_seg->seg_offset;
		f_seg->seg_offset = get_new_seg(f_seg->manager);
		f_seg->header.next = f_seg->seg_offset;
		simplewrite(write_offset, &(f_seg->header), sizeof(struct file_seg_header), f_seg->manager->f);
		f_seg->header.previous = write_offset;
		write_offset += sizeof(struct file_seg_header);
		simplewrite(write_offset, f_seg->file, sizeof(struct file) * FILE_PER_SEG, f_seg->manager->f);
		f_seg->len = 0;
	}
	f_seg->file[f_seg->len] = f;
	f_seg->len ++;
	return 0;
}

int get_files(struct file * f, int len, int pos, struct file_seg * f_seg)
{
	int got;
	int diff;
	uint64_t read_offset;
	uint64_t seg_offset;
	struct file_seg_header header;
	if(NULL == f)
		return -1;
	got = 0;
	if(ALL == pos)
	{
		seg_offset = f_seg->start_seg_offset;
		while(seg_offset != f_seg->seg_offset)
		{
			read_offset = seg_offset + sizeof(struct file_seg_header);
			simpleread(read_offset, f, FILE_PER_SEG * sizeof(struct file), f_seg->manager->f);
			simpleread(seg_offset, &header, sizeof(struct file_seg_header), f_seg->manager->f);
			seg_offset = header.next;
			f += FILE_PER_SEG;
			got += FILE_PER_SEG;
		}
		memcpy((void *)f, (const void *)f_seg->file, (size_t)f_seg->len * sizeof(struct file));
		got += f_seg->len;
		return got;
	}
	if(END == pos)
	{
		if(len > f_seg->len)
		{
			memcpy(f, f_seg->file, sizeof(struct file) * f_seg->len);
			len -= f_seg->len;
			f += f_seg->len;
			got += f_seg->len;
		}
		else
		{
			diff = f_seg->len - len;
			memcpy(f, &(f_seg->file[diff]), sizeof(struct file) * len);
			got += len;
			len -= len;
		}
		seg_offset = f_seg->header.previous;
		while(len > 0)
		{
			if(len > FILE_PER_SEG)
			{
				read_offset = seg_offset + sizeof(struct file_seg_header);
				simpleread(read_offset, f, FILE_PER_SEG * sizeof(struct file), f_seg->manager->f);
				simpleread(seg_offset, &header, sizeof(struct file_seg_header), f_seg->manager->f);
				seg_offset = header.previous;
				f += FILE_PER_SEG;
				got += FILE_PER_SEG;
				len -= FILE_PER_SEG;
			}
			else
			{
				diff = FILE_PER_SEG - len;
				read_offset = seg_offset + sizeof(struct file_seg_header) + sizeof(struct file) * diff;
				simpleread(read_offset, f, len * sizeof(struct file), f_seg->manager->f);
				got += len;
				len -= len;
			}
		}
	}
	if(START == pos)
	{
		seg_offset = f_seg->start_seg_offset;
		while(len > 0)
		{
			if(seg_offset != f_seg->seg_offset)
			{
				if(len > FILE_PER_SEG)
				{
					read_offset = seg_offset + sizeof(struct file_seg_header);
					simpleread(read_offset, f, FILE_PER_SEG * sizeof(struct file), f_seg->manager->f);
					simpleread(seg_offset, &header, sizeof(struct file_seg_header), f_seg->manager->f);
					seg_offset = header.next;
					f += FILE_PER_SEG;
					got += FILE_PER_SEG;
					len -= FILE_PER_SEG;
				}
				else
				{
					read_offset = seg_offset + sizeof(struct file_seg_header);
					simpleread(read_offset, f, len * sizeof(struct file), f_seg->manager->f);
					got += len;
					len -= len;
				}
			}
			else
			{
				if(len > f_seg->len)
				{
					memcpy(f, f_seg->file, f_seg->len * sizeof(struct file));
					got += f_seg->len;
					len -= f_seg->len;
					break;
				}
				else
				{
					memcpy(f, f_seg->file, len * sizeof(struct file));
					got += len;
					len -= len;
				}
				break;
			}
		}
	}
	return got;
}


int get_file_by_name(struct file * f, struct file_seg * f_seg, char *name)
{
	int got, i;
	uint64_t read_offset;
	uint64_t seg_offset;
	struct file *fs;
	struct file_seg_header header;
	if(NULL == f)
		return -1;
	got = 0;
	seg_offset = f_seg->start_seg_offset;
	fs = malloc(FILE_PER_SEG*sizeof(struct file));
	while(seg_offset != f_seg->seg_offset)
	{
		read_offset = seg_offset + sizeof(struct file_seg_header);
		simpleread(read_offset, fs, FILE_PER_SEG * sizeof(struct file), f_seg->manager->f);
		simpleread(seg_offset, &header, sizeof(struct file_seg_header), f_seg->manager->f);
		seg_offset = header.next;
		for(i=0;i<FILE_PER_SEG;i++)
		{
			if(strcmp(fs[i].name, name)==0)
			{
				memcpy(f, &fs[i], sizeof(struct file));
				free(fs);
				return 1;
			}
		}
	}
	memcpy((void *)fs, (const void *)f_seg->file, (size_t)f_seg->len * sizeof(struct file));
	for(i=0;i<f_seg->len;i++)
	{
		if(strcmp(fs[i].name, name)==0)
		{
			memcpy(f, &fs[i], sizeof(struct file));
			free(fs);
			return 1;
		}
	}
	free(fs);
	return 0;
	
}



