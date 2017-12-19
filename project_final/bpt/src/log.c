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
	char * pathname = "my_log.txt"

	create_log_buf();
	
	// If log file exists
	if (access(pathname, 0) == 0) {
		if ((log_fd = open(pathname, O_RDWR | O_SYNC, 0644)) == -1) {
			// fail to access
			printf("fail to access log file\n");
			return;
		} else {
			recovery_from_file();
		}
	 } else {
		 // If log file doesn't exist, make file.
		 if (log_fd = open(pathname, O_CREAT | O_RDWR | O_SYNC, 0644)) == -1) {
			// fail to make file.
			printf("faile to make log file\n");
			return;
		 }
	 }
}

// Recovery from log file.
void recovery_from_file() {
	internal_page * page;
	Buf * b;
	bool is_trx;
	Log * log;
	int64_t log_offset;

	flush_lsn = 0;
	log_offset = 0;
	is_trx = false;
	log = (Log *)malloc(sizeof(Log));

	// redo phase	
	while (pread(log_fd, log, LOG_SIZE, log_offset) > 0) {
		switch(log->type) {
			case BEGIN : 
				is_trx = true;
				break;
			case UPDATE : 
				b = get_buf(log->table_id, (log->page_num * PAGE_SIZE));
				page = (internal_page *)b->page;

				if (page->page_lsn >= log->lsn)
					break;
				else {
					b->page = log->new_image;
					mark_dirty(b);
					release_pincount(b);
					break;
				} 
			case COMMIT : 
				is_trx = false;
				break;
			case ABORT : 
				break;
			case ROLLBACK : 
				is_trx = false;
				break;
		}
		log_offset += LOG_SIZE;
	} 

	// uncommited trx exists
	if (is_trx)
		rollback(log->lsn);

	free(log);
}

void rollback(int64_t lsn) {
	Log * log;
	Buf * b;
	log = (Log *)malloc(sizeof(Log));

	while (pread(log_fd, log, LOG_SIZE, lsn) > 0) {
		if (log->type == BEGIN || log->type == ABORT) {
			create_log(b, ROLLBACK);
			complete_log(b, ROLLBACK);
			break;
		}
		b = get_buf(log->table_id, (log->page_num) * PAGE_SIZE);
		b->page = log->old_image;
		mark_dirty(b);
		release_pincount(b);
		lsn = log->prev_lsn;
	}
}






