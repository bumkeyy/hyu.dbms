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
	for (i = 0; i < LOG_BUFFER_SIZE; i++) {
		log_buf[i].old_image = (Page *)malloc(sizeof(Page));
		log_buf[i].new_image = (Page *)malloc(sizeof(Page));
	}
	flushed_num = LOG_INIT_NUM;
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

	if (type == UPDATE) {
		log->table_id = b->table_id;
		log->page_num = (b->page_offset) / PAGE_SIZE;
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
	for (i = flushed_num + 1; i < num; i++) {
		write(log_fd, &log_buf[i], LOG_HEADER_SIZE);
		write(log_fd, &log_buf[i].old_image, PAGE_SIZE);
		write(log_fd, &log_buf[i].new_image, PAGE_SIZE);
	}

	flushed_num = num;
	if (num = LOG_BUFFER_SIZE - 1)
		flushed_num = LOG_INIT_NUM;
}

// Initialize log buffer and
// check recovery.
void init_log() {
	char * pathname = "minidb.log";

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
		 if ((log_fd = open(pathname, O_CREAT | O_RDWR | O_SYNC, 0644)) == -1) {
			// fail to make file.
			printf("faile to make log file\n");
			return;
		 }
	 }
}

// Recovery from log file.
void recovery_from_file() {
	char * name_table[11] = {
		"DATA0",
		"DATA1",
		"DATA2",
		"DATA3",
		"DATA4",
		"DATA5",
		"DATA6",
		"DATA7",
		"DATA8",
		"DATA9",
		"DATA10"
	};

	internal_page * page;
	Buf * b;
	bool is_trx;
	Log * log;
	int64_t log_offset;

	flushed_lsn = 0;
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
				if (table[log->table_id] == 0) 
					open_table(name_table[log->table_id]);
				b = get_buf(log->table_id, (log->page_num * PAGE_SIZE));
				page = (internal_page *)b->page;
				if (page->page_lsn >= log->lsn) {
					//close(table[log->table_id]);
					break;
				}
				else {
					b->page = log->new_image;
					page = (internal_page *)b->page;
					page->page_lsn = log->lsn;
					mark_dirty(b);
					release_pincount(b);
					//close(table[log->table_id]);
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
		//rollback(log->lsn);

	free(log);
}

void rollback(int64_t lsn) {
	Log * undo_log;
	Buf * b;
	internal_page * page;
	undo_log = (Log *)malloc(sizeof(Log));

	while (pread(log_fd, undo_log, LOG_SIZE, lsn) > 0) {
		if (undo_log->type == BEGIN) {
			create_log(b, ROLLBACK);
			complete_log(b, ROLLBACK);
			break;
		}
		b = get_buf(undo_log->table_id, (undo_log->page_num) * PAGE_SIZE);
		page = (internal_page *)b->page;
		if (page->page_lsn > undo_log->lsn) {
			b->page = undo_log->old_image;
			page = (internal_page *)b->page;
			mark_dirty(b);
			release_pincount(b);
		}
		lsn = undo_log->prev_lsn;
	}
	free(undo_log);		
}

int update(int table_id, int64_t key, char * value) {

	Buf * b;
	leaf_page * leaf;
	int i;

	if (find(table_id, key) == NULL) {
		return -1;
	}

	b = find_leaf(table_id, key);
	leaf = (leaf_page *)b->page;

	if (trx)
		leaf->page_lsn = create_log(b, UPDATE);
	printf("Update page_lsn : %lld \n", leaf->page_lsn);

	for (i = 0; i < leaf->num_keys; i++) {
		if (leaf->records[i].key == key)
			break;
	}

	memcpy(leaf->records[i].value, value, VALUE_SIZE);

	if(trx)
		complete_log(b, UPDATE);

	mark_dirty(b);
	release_pincount(b);

	return 0;	
}

int begin_transaction() {
	Buf * b;
	trx = true;
	create_log(b, BEGIN);
	complete_log(b, BEGIN);
	return 0;
}

int commit_transaction() {
	Buf * b;
	if (trx) {
		create_log(b, COMMIT);
		complete_log(b, COMMIT);
		if (end_num != LOG_INIT_NUM)
			flush_log(end_num);
	}
	trx = false;
	return 0;
}

int abort_transaction() {
	Buf * b;
	if (trx) {
		rollback(log_buf[end_num].lsn);
		create_log(b, ROLLBACK);
		complete_log(b, ROLLBACK);
	}
	trx = false;
	return 0;
}






