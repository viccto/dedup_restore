#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <dirent.h>
#include <string.h>

#include "enums.h"
#include "config.h"
#include "adler32.h"
#include "sha1.h"
#include "bloomfilter.h"
#include "storagemanager.h"
#include "file.h"
#include "diskhash.h"
#include "data.h"
#include "container.h"
#include "list.h"
#include "metadata.h"
#include "cache.h"
#ifdef COMPRESS
#include "lzjb.h"
#endif
#include "dedup.h"
#include "chunk.h"



/* the function is used for newly created file dedup
 * store the metadata in fp, which is the metadata file
 * in re directory */
int newly_chunk(char * chunk_buf, char * data, size_t len, int cont, int file_end, struct dedup_manager * de_dup, FILE *fp)
{
	dedup_log("enter newly_chunk\n");
	static char first;
	static int in_chunk_len = 0;
	static uint32_t abstract;
	static char * win_start;
	char new;
	uint32_t chunk_num;

	int parsed_len;
	int cpy_len;

	if(NULL == chunk_buf)
		return -1;
	if(NULL == data)
		return -1;

	parsed_len = 0;
	//if the data is not continued with the previous one, clean the data in chunk_buf
	if(0 == cont)
	{
		in_chunk_len = 0;
	}
	chunk_num = 0;
	while(parsed_len < len)
	{
		if(in_chunk_len < MIN_CHUNK_LEN)
		{
			cpy_len = MIN_CHUNK_LEN - in_chunk_len;
			if(cpy_len > (len - parsed_len))
			{
				cpy_len = len - parsed_len;
				memcpy(chunk_buf + in_chunk_len, data + parsed_len, cpy_len);
				in_chunk_len += cpy_len;
				parsed_len += cpy_len;
			}
			else
			{
				memcpy(chunk_buf + in_chunk_len, data + parsed_len, cpy_len);
				in_chunk_len += cpy_len;
				parsed_len += cpy_len;

				win_start = chunk_buf + MIN_CHUNK_LEN - WIN_LEN;
				first = * win_start;

				abstract = adler32_checksum(win_start, WIN_LEN);
				if((abstract & CHUNK_MASK) == CHUNK_MASK)
				{
					// a chunk
					newly_dedup(chunk_buf, in_chunk_len, de_dup, fp);
					in_chunk_len = 0;
					chunk_num ++;
				}
			}
		}
		else
		{
			new = *(data + parsed_len);
			chunk_buf[in_chunk_len] = data[parsed_len];
			in_chunk_len ++;
			parsed_len ++;

			if(in_chunk_len >= MAX_CHUNK_LEN)
			{
				// a chunk
				newly_dedup(chunk_buf, in_chunk_len, de_dup, fp);
				in_chunk_len = 0;
				chunk_num ++;
			}

			first = * win_start;
			abstract = adler32_rolling_checksum(abstract, WIN_LEN, first, new);
			win_start ++;

			if((abstract & CHUNK_MASK) == CHUNK_MASK)
			{
				// a chunk
				newly_dedup(chunk_buf, in_chunk_len, de_dup, fp);
				in_chunk_len = 0;
				chunk_num ++;
			}
		}
	}

	//file ends the last chunk
	if(0 != file_end)
	{
		if(in_chunk_len > 0)
		{
			newly_dedup(chunk_buf, in_chunk_len, de_dup, fp);
			in_chunk_len = 0;
			chunk_num ++;
		}
	}
	return chunk_num;
}



