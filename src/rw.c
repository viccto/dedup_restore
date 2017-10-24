#include <stdio.h>
#include <ulockmgr.h>
#include <pthread.h>
#include <limits.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <time.h>

#include "config.h"
#include "optsmr.h"
#include "smr.h"
#include "rw.h"


#define OPTSMR_IO_SIZE (1024*1024*16)


int32_t optsmr_get_data(int64_t iosize, char *buf)
{
	rw_log("optsmr_get_data\n");
	FILE *fp;
	fp = fopen("/home/duadmin/gctest/11m.log", "r");
	if(fp==NULL){
		error_log("open file error\n");
		return -1;
	}
	return fread(buf, iosize, 1, fp);
}
	

int32_t optsmr_set_data(int64_t iosize, char *buf)
{
	if(buf==NULL)
		return;
	int32_t setsize;
	setsize = iosize/(sizeof(char));
	memset(buf,'w', setsize);
}	

int32_t optsmr_reset_pointer(int32_t zoneidx)
{
	if(zoneidx<0){
		error_log("zoneidx false\n");
		return -1;
	}
	char shell[100]="zbc_reset_write_ptr /dev/sdd ";
	char zone[20];
	sprintf(zone, "%d", zoneidx);
	strcat(shell,zone);
	system(shell);
	return 0;
}

double optsmr_write(int64_t iosize, char * buf, int32_t io_num, int32_t start_zone)
{
	rw_log("optsmr_write\n");
	int64_t wp_lba, lba;
	int32_t zoneidx, *w_zoneidx, w_size;
	int i, zone_num, zone_write;
	struct timeval tpstart,tpend;
	double timeuse, total_time, iosize_MB, io;
	io = (double)iosize;
	iosize_MB = io/(1024*1024);	

	lba = start_zone*SMR_LBA_PER_ZONE;
	/*
	zone_num = 2+(iosize*io_num)/SMR_ZONE_SIZE;
	//smr_reset_pointer(zoneidx);
	zoneidx = start_zone;
	printf("zone_num = %d, and zoneidex = %d\n",zone_num, zoneidx);
	for(i=0;i<zone_num;i++){
		printf("reset pointer zone_num = %d, and zoneidex = %d\n",zone_num, zoneidx);
		optsmr_reset_pointer(zoneidx);
		zoneidx +=1;
	}
	*/
	total_time = 0;
	zone_write = start_zone;
	int j=0;
	for(i=0;i<io_num;i++)
	{
		/*
		gettimeofday(&tpstart, NULL);
		w_size = smr_lba_write(lba, i, buf, iosize);
		gettimeofday(&tpend, NULL);
		timeuse=(1000000*(tpend.tv_sec-tpstart.tv_sec)+tpend.tv_usec-tpstart.tv_usec);
		smr_log("time used to write  %fMB data is: %f microsecond, throuhgput is:%fMB/S\n", iosize_MB, timeuse/1000000, (iosize_MB/timeuse)*1000000);
		lba +=w_size/SMR_LOGICAL_BLK_SIZE;
		rw_log("write the size: %ld, the next start lba is : %ld\n", w_size, lba);
		total_time +=timeuse;
		*/
		w_size = smr_lba_write(lba, i, buf, iosize);
		lba +=w_size/SMR_LOGICAL_BLK_SIZE;
		j +=1;
		if(j==16){
			result_log("write the zone: %d\n", zone_write);
			zone_write +=1;
			j=0;
		}
	}
	return total_time/1000000;
}
	
int64_t optsmr_range_random(int64_t start, int64_t end)
{
	int64_t  ran_ret;
	double range = end-start;
	ran_ret = start+(int64_t)(range*rand()/(RAND_MAX+1.0));
	return ran_ret;
	
}

void optsmr_read_print(int64_t lba, int64_t iosize, int32_t io_num)
{
	rw_log("optsmr_read_print, lba:%ld, iosize: %ld\n", lba,iosize);
	char *buf;
	buf = malloc(iosize);
	int32_t setsize;
	struct timeval tpstart,tpend;
	double timeuse, total_time, iosize_MB, io;
    io = (double)iosize;
    iosize_MB = io/(1024*1024);

    setsize = iosize/(sizeof(char)); 
    memset(buf,'-', setsize);
	printf("the read num: %d, the content: <%c>\n", setsize, buf[4]);
	int i;
	total_time = 0;
	for(i=0;i<io_num;i++){
		gettimeofday(&tpstart, NULL);
		setsize = smr_lba_read(lba, 1, buf, iosize);
		gettimeofday(&tpend, NULL);
		timeuse=(1000000*(tpend.tv_sec-tpstart.tv_sec)+tpend.tv_usec-tpstart.tv_usec);
		printf("time used to read  %fMB data is: %f microsecond, throuhgput is:%fMB/S\n", iosize_MB, timeuse/1000000, (iosize_MB*1000000)/timeuse);
		//printf("the read lba: %ld, read num: %d, the content: <%s>\n", lba, setsize, buf);
		lba += iosize/SMR_LOGICAL_BLK_SIZE;
		total_time +=timeuse;
	}
	printf("the everage read throughput is: %fMB/S\n", (io_num*iosize_MB*1000000)/total_time);
	free(buf);
}

double optsmr_one_lba_read(int64_t lba, int64_t iosize, char *buf)
{
	rw_log("optsmr_one_lba_read, lba:%ld, iosize: %ld\n", lba,iosize);
    int32_t setsize;
    struct timeval tpstart,tpend;
    double timeuse, total_time, iosize_MB, io;
    io = (double)iosize;
    iosize_MB = io/(1024*1024);

    setsize = iosize/(sizeof(char));
    int i;
    total_time = 0;
    gettimeofday(&tpstart, NULL);
    setsize = smr_lba_read(lba, 1, buf, iosize);
    gettimeofday(&tpend, NULL);
    timeuse=(1000000*(tpend.tv_sec-tpstart.tv_sec)+tpend.tv_usec-tpstart.tv_usec);
    time_log("time used to read  %fMB data is: %f microsecond, throuhgput is:%fMB/S\n", iosize_MB, timeuse/1000000, (iosize_MB*1000000)/timeuse);
	total_time = timeuse/1000000;
	return total_time;
}

double optsmr_range_random_read(int64_t start_lba, int64_t end_lba, int64_t iosize, int64_t io_num)
{
	int64_t i, lba;
	double time, total_time;
	char *buf;
	buf = malloc(iosize);
	total_time = 0;
	for(i=0;i<io_num;i++)
	{
		lba = optsmr_range_random(start_lba, end_lba);
		time = optsmr_one_lba_read(lba, iosize, buf);
		total_time +=time;
	}
	return total_time;
}

void optsmr_rw_test(int32_t start_zone, int64_t w_iosize, int64_t w_num, int64_t r_iosize, int64_t r_num)
{
	double w_mb, r_mb, w_time, r_time;
	int64_t start_lba, end_lba;
	char *buf;
	buf = malloc(w_iosize);
	optsmr_set_data(w_iosize, buf);
	start_lba = start_zone*SMR_LBA_PER_ZONE;
	end_lba = start_lba+(w_iosize*w_num-r_iosize)/SMR_LOGICAL_BLK_SIZE;
	w_mb = (double)((w_iosize*w_num)/(1024*1024));
	w_time = optsmr_write(w_iosize, buf, w_num, start_zone);
	result_log("total write: %fMB data, iosize is: %ld, the average write throughput is: %f\n",w_mb, w_iosize, w_mb/w_time);

	int i;
	int64_t read_iosize;
	read_iosize = 1024;
	for(i=0;i<15;i++)
	{
		r_mb = (double)((read_iosize*r_num)/(1024*1024));
		r_time = optsmr_range_random_read(start_lba, end_lba, read_iosize, r_num);
		result_log("total write: %fMB data, the read size is: %ld, the read io number is: %ld, the average read throughput is: %f\n",r_mb, read_iosize, r_num, r_mb/r_time);
		read_iosize = read_iosize*2;
	}
	free(buf);
}

void optsmr_seq_write(int32_t start_zone, int64_t w_iosize, int64_t w_num)
{
    double w_mb, w_time;
    char *buf;
    buf = malloc(w_iosize);
    optsmr_set_data(w_iosize, buf);
    w_mb = (double)((w_iosize*w_num)/(1024*1024));
    //system("/home/duadmin/zhichao/optsmr/reset.sh");
    w_time = optsmr_write(w_iosize, buf, w_num, start_zone);
    result_log("total write: %fMB data, iosize is: %ld, the average write throughput is: %f\n",w_mb, w_iosize, w_mb/w_time);
    free(buf);
}

double optsmr_order_read(int64_t *read_seq, int64_t r_iosize, int64_t r_num)
{
	double r_time, iosize_MB;
	struct timeval tpstart,tpend;
	char *buf;
	int i;
	buf = malloc(r_iosize);
	iosize_MB = ((double)r_iosize*r_num)/(1024*1024);	

	gettimeofday(&tpstart, NULL);
	for(i=0;i<r_num;i++)
	{
		smr_lba_read(*(read_seq+i), 1, buf, r_iosize);
	}
	gettimeofday(&tpend, NULL);
	r_time=(1000000*(tpend.tv_sec-tpstart.tv_sec)+tpend.tv_usec-tpstart.tv_usec);
	//time_log("time used to read  %fMB data is: %f second, throuhgput is:%fMB/S\n", iosize_MB, r_time/1000000, (iosize_MB*1000000)/r_time);
	free(buf);
	return r_time/1000000;
}

void optsmr_range_read(int64_t start_zone, int64_t end_zone, int64_t iosize, int64_t io_num)
{
	int64_t *read_seq;
	int64_t i, zone_max_ofst, pre_zone, pre_lba, zone_distance, lba_distance, read_iosize;
	double r_mb, r_time;
	read_seq = (int64_t *)malloc(io_num*3*sizeof(int64_t));
	zone_max_ofst = SMR_LBA_PER_ZONE-iosize/SMR_LOGICAL_BLK_SIZE-1;
	pre_zone = 0;
	pre_lba = 0;
	zone_distance = 0;
	lba_distance = 0;
	srand((int)time(0));
	for(i=0;i<io_num;i++)
	{
		*(read_seq+io_num+i) = optsmr_range_random(start_zone, end_zone);
		*(read_seq+io_num*2+i) = optsmr_range_random(0, zone_max_ofst);
		*(read_seq+i) = (*(read_seq+io_num+i))*SMR_LBA_PER_ZONE+*(read_seq+io_num*2+i);
		zone_distance = zone_distance+abs(*(read_seq+io_num+i)-pre_zone);
		lba_distance = lba_distance+abs(*(read_seq+i)-pre_lba);
		pre_zone = *(read_seq+io_num+i);
		pre_lba = *(read_seq+i);
		//printf("the zone: %ld, the lba offset: %ld, the lba: %ld\n", *(read_seq+io_num+i), *(read_seq+io_num*2+i), *(read_seq+i));
	}
	result_log("%ld\n",zone_distance);
	read_iosize = 1024;
	for(i=0;i<16;i++)
	{
		r_mb = (((double)read_iosize*io_num)/(1024*1024.0));
		r_time = optsmr_order_read(read_seq, read_iosize, io_num);
		//printf("total write: %fMB data, the read size is: %ld, the read io number is: %ld, the average read throughput is: %f\n",r_mb, read_iosize, io_num, r_mb/r_time);
		printf("%f\n",r_mb/r_time);
		result_log("%f\n",r_mb/r_time);
		read_iosize = 2*read_iosize;
	}
	result_log("the total zone_distance: %ld, the total lba_distance: %ld\n", zone_distance, lba_distance);
}


