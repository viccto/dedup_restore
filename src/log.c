/*
 * log.c the log source code of the optsmr
 * created date: 2017/02/03
 * author: Zhichao Cao
*/

#include <libzbc/zbc.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>

#include "enums.h"
#include "config.h"
#include "optsmr.h"
#include "smr.h"
#include "log.h"

FILE *log_open()
{
	FILE *logfile;
	logfile = fopen("optsmr.log","w");
	if(logfile==NULL){
		return NULL;
	}

	setvbuf(logfile, NULL, _IOLBF, 0);
	return logfile;
}

void smr_log(const char *format, ...)
{
	va_list ap;
    va_start(ap, format);
#ifdef SMR_DEBUG
    fprintf(GET_FPDD_DATA()->logfile, "smr:");
    vfprintf(GET_FPDD_DATA()->logfile, format, ap);
#endif
}


void rw_log(const char *format, ...)
{
    va_list ap;
    va_start(ap, format);
#ifdef RW_DEBUG
    fprintf(GET_FPDD_DATA()->logfile, "rw:");
    vfprintf(GET_FPDD_DATA()->logfile, format, ap);
#endif
}

void error_log(const char *format, ...)
{
    va_list ap;
    va_start(ap, format);
    fprintf(GET_FPDD_DATA()->logfile, "error:");
    vfprintf(GET_FPDD_DATA()->logfile, format, ap);
}

void time_log(const char *format, ...)
{
    va_list ap;
    va_start(ap, format);
#ifdef TIME_DEBUG
    //fprintf(GET_FPDD_DATA()->logfile, "time:");
    vfprintf(GET_FPDD_DATA()->logfile, format, ap);
#endif
}

void result_log(const char *format, ...)
{
    va_list ap;
    va_start(ap, format);
#ifdef RESULT_PRINT
    //fprintf(GET_FPDD_DATA()->logfile, "time:");
    vfprintf(GET_FPDD_DATA()->logfile, format, ap);
#endif
}


void dedup_log(const char *format, ...)
{
	va_list ap;
    va_start(ap, format);
#ifdef DEDUP_DEBUG
    vfprintf(GET_FPDD_DATA()->logfile, format, ap);
#endif
}

void filemeta_log(const char *format, ...)
{
	va_list ap;
    va_start(ap, format);
#ifdef FILEMETA_DEBUG
    vfprintf(GET_FPDD_DATA()->logfile, format, ap);
#endif
}

void container_log(const char *format, ...)
{
        va_list ap;
    va_start(ap, format);
#ifdef CONTAINER_DEBUG
    vfprintf(GET_FPDD_DATA()->logfile, format, ap);
#endif
}

void gc_log(const char *format, ...)
{
        va_list ap;
    va_start(ap, format);
#ifdef GC_DEBUG
    vfprintf(GET_FPDD_DATA()->logfile, format, ap);
#endif
}

void test_log(const char *format, ...)
{
        va_list ap;
    va_start(ap, format);
#ifdef TEST_DEBUG
    vfprintf(GET_FPDD_DATA()->logfile, format, ap);
#endif
}

void look_ahead_log(const char *format, ...)
{
        va_list ap;
    va_start(ap, format);
#ifdef LOOK_AHEAD_DEBUG
    vfprintf(GET_FPDD_DATA()->logfile, format, ap);
#endif
}


void memstore_log(const char *format, ...)
{
        va_list ap;
    va_start(ap, format);
#ifdef MEMSTORE_DEBUG
    vfprintf(GET_FPDD_DATA()->logfile, format, ap);
#endif
}


