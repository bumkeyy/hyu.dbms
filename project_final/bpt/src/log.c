/**
 *	@class Database System
 *	@file  log.c
 *	@brief Log manager & Recovery 
 *	@author Kibeom Kwon (kgbum2222@gmail.com)
 *	@since 2017-12-17
 */     

#include "bpt.h"

// Create log buffer.
void create_log_buf() {
	int i;
	log_buf = (Log *) malloc(sizeof(Log) * LOG_BUFFER_SIZE);
	flush_num = LOG_INIT_NUM;
	end_num = LOG_INIT_NUM;
}

// Create Log record
int create_log(Buf * b, LOG_TYPE type) {
	int cur;
	Log * log;

	// current index
	cur = end_num + 1;
	if (cur == LOG_BUFFER_SIZE)
		cur = 0;
	
	log = &log_buf[cur];

	// set lsn and prev_lsn
	if (end_num == LOG_INIT_NUM) {
		log->lsn = 0;
		log->prev_lsn = 0;
	} else {
		log->lsn = log_buf[end_num].lsn + LOG_SIZE;
		log->prev_lsn = log_buf[end_num].lsn;
	}

	log->trx_id = trx_id;
	log->type = type;
	log->table_id = b->table_id;

	if (type == UPDATE) {
		log->page_num = (b->offset) / PAGE_SIZE;
		log->offset = 0;
		log->length = PAGE_SIZE;
		log->old_image = b->page;
	}

	// BEGIN, COMMIT, ABORT are returned right away.
	return log->lsn;
}

// Complete log record
int complete_log(Buf * b, LOG_TYPE type) {
	
	// current index
	int cur = end_num + 1;

	if (cur == LOG_BUFFER_SIZE)
		cur = 0;

	Log * log = &log_buf[cur];

	if (type == UPDATE) 
		log->new_image = b->page;

	if (end_num == LOG_BUFFER_SIZE - 1)
		flush_log(end_num);

	end_num = cur;
	return 0;
}

// Flush log buffer.
// from flush_lsn to num.
void flush_log(int num) {
	int i;
	for (i = flush_num + 1; i < num; i++) 
		write(log_fd, &log_buf[i], LOG_SIZE);

	flush_num = num;
	if (num = LOG_BUFFER_SIZE - 1)
		flush_num = -1;
}

// Initialize log buffer and
// check recovery.
void init_log() {
	create_log_buf();

}







