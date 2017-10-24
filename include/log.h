/*
 * log.h
 * created date: 2017/02/03
 * author: Zhichao Cao
*/

#ifndef _LOG_H_
#define _LOG_H_

FILE *log_open(void);
void smr_log(const char *format, ...);

void rw_log(const char *format, ...);

void error_log(const char *format, ...);

void time_log(const char *format, ...);

void result_log(const char *format, ...);

void dedup_log(const char *format, ...);

void filemeta_log(const char *format, ...);

void container_log(const char *format, ...);

void gc_log(const char *format, ...);

void test_log(const char *format, ...);

void look_ahead_log(const char *format, ...);

void memstore_log(const char *format, ...);

#endif
