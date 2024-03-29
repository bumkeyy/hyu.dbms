/**
 *		@class Database System
 *		@file  buffer.c
 *		@brief Log manager & Recovery
 *		@author Kibeom Kwon (kgbum2222@gmail.com)
 *		@since 2017-12-17
 */

#include "bpt.h"

Buf * read_headerpage(int table_id) {
	int i = 0;
	Buf * hb;
	// Find buffer frame to use
	while (1) {
		if (i == num_buf) {
			make_victim();
			i = 0;
		}
		if (buf[i].page_offset == PAGE_NONE)
			break;
		i++;
	}
	// Register header page to buffer frame.
	hb = &buf[i];
	read_page(table_id, hb->page, PAGE_SIZE, HEADERPAGE_OFFSET);
	hb->table_id = table_id;
	hb->page_offset = HEADERPAGE_OFFSET;
	hb->pin_count = 1;
	hb->in_LRU = false;
	hb->is_dirty = 1;

	return register_header_LRU(hb); 
}

Buf * register_header_LRU(Buf * hb) {

	if (LRU_list->num_lru >= num_buf && hb->in_LRU == false) {
		make_victim();
	}

	// Update LRU
	hb->lru->next = LRU_list->head->next;
	LRU_list->head->next->prev = hb->lru;
	hb->lru->prev = LRU_list->head;
	LRU_list->head->next = hb->lru;
	hb->in_LRU = true;
	LRU_list->num_lru++;

	return hb;
}

// Initialize headerpage.
Buf * init_headerpage (int table_id) {
	int i;
	Buf * hb;
	// Find buffer frame to use
	for (i = 0; i < num_buf; i++)
		if (buf[i].page_offset == PAGE_NONE)
			break;
	// Register header page to buffer frame.
	hb = &buf[i];
	hb->table_id = table_id;
	hb->page_offset = HEADERPAGE_OFFSET;
	hb->pin_count = 1;
	hb->is_dirty = 1;

	return register_header_LRU(hb);
}

// Release pin_count of page.
void release_pincount(Buf * b) {
	b->pin_count--;
}

// Mark dirty bit of page.
void mark_dirty(Buf * b) {
	b->is_dirty = true;
}

// Initialize all buffer frame
void init_buf(int i) {
	buf[i].page = (Page *) malloc(sizeof(Page));
	buf[i].table_id = 0;
	buf[i].page_offset = PAGE_NONE;
	buf[i].is_dirty = false;
	buf[i].pin_count = 0;
	buf[i].lru = (LRU *) malloc(sizeof(LRU));
}

// Initialize LRU_list.
void init_LRU(void) {
	LRU_list = (LRU_LIST *)malloc(sizeof(LRU_LIST));
	LRU * dummy_head = (LRU *)malloc(sizeof(LRU));
	LRU * dummy_tail = (LRU *)malloc(sizeof(LRU));
	dummy_head->next = dummy_tail;
	dummy_tail->prev = dummy_head;
	LRU_list->head = dummy_head;
	LRU_list->tail = dummy_tail;
	LRU_list->num_lru = 0;
}


/* Allocate the buffer pool (array) with the given number of entries.
 * Initialize other fields (state info, LRU info..) with your own design.
 * If success, return 0. Otherwise, return non-zero value.
 */
int init_db(int num) {
	int i;
	num_buf = num;
	buf = (Buf *) malloc(sizeof(Buf) * num_buf);

	for (i = 0; i < num_buf; i++)
		init_buf(i);

	init_LRU();
	init_log();

	return 0;
}


/* Make victim page and remove.
 */
void make_victim() {
	int i;
	Buf * vb;
	LRU * d_lru;
	internal_page * page;

	while (1) {
		d_lru = LRU_list->tail->prev;
		// Remove in LRU_list.
		d_lru->prev->next = LRU_list->tail;
		LRU_list->tail->prev = d_lru->prev;

		vb = d_lru->buf;

		if (vb->pin_count != 0) {
			// If victim page is using, current page moves to head of LRU_li     st
			d_lru->next = LRU_list->head->next;
			LRU_list->head->next->prev = d_lru;
			d_lru->prev = LRU_list->head;
			LRU_list->head->next = d_lru;
			continue;
		}
		break;
	}

	page = (internal_page *)vb->page;
	// WAL
	if (page->page_lsn > 0) {
		for (i = flushed_num; i < end_num; i++) 
			if (log_buf[i].header->lsn > page->page_lsn)
				break;
		//printf("eviction!!!\n");
		if (i >= 0)
			flush_log(i);
	}

	if (vb->is_dirty) {
		write_page(vb->table_id, vb->page, PAGE_SIZE, vb->page_offset);
	}
		
	vb->is_dirty = false;
	vb->in_LRU = false;
	vb->page_offset = PAGE_NONE;

	LRU_list->num_lru--;
}


/* Upate LRU.
 * If success return 0. Otherwise, return non_zero value.
 */
int update_LRU(Buf * b) {
	if (LRU_list->num_lru >= num_buf && b->in_LRU == false) {
		make_victim();
	}

	b->lru->buf = b;
	// If page already exists in buffer.
	if (b->in_LRU == true) {
		b->lru->prev->next = b->lru->next;
		b->lru->next->prev = b->lru->prev;
		LRU_list->num_lru--;
	}

	b->lru->next = LRU_list->head->next;
	LRU_list->head->next->prev = b->lru;
	b->lru->prev = LRU_list->head;
	LRU_list->head->next = b->lru;
	b->in_LRU = true;

	LRU_list->num_lru++;
	if (LRU_list->num_lru > num_buf) {
	//	printf("update_LRU() error!!!!\n");
		return -1;
	}

	b->pin_count++;

	return 0;
}


/* Find Buf structure of its offset.
 * If exist, return buf pointer.
 * If not, return NULL
 */
Buf * find_buf(int table_id, int64_t offset) {
	int i;

	// Find page in buffer pool.
	for (i = 0; i < num_buf; i++) {
		if (buf[i].table_id == table_id && buf[i].page_offset == offset) {
			update_LRU(&buf[i]);
			return &buf[i];
		}
	}

	// If not exist
	return NULL;
}


int get_free_buffer_index(void) {
	int i;
	// If buffer is full
	if (LRU_list->num_lru == num_buf) {
		make_victim();
		for (i = 0; i < num_buf; i++) {
			if (buf[i].page_offset == PAGE_NONE) {
				break;
			}
		}
	}   // If not
	else {
		for (i = 0; i < num_buf; i++) {
			if (buf[i].page_offset == PAGE_NONE) {
				break;
			}
		}
	}

	return i;
}

void alloc_freepage(int table_id, Buf * hb, int64_t offset) {
	int64_t free_offset;
	header_page * hp;
	free_page * fp;

	free_offset = offset;
	hp = (header_page *)hb->page;
	fp = (free_page *)malloc(sizeof(free_page));
	read_page(table_id, (Page *)fp, PAGE_SIZE, offset);

	if (fp->next_page == 0) {
		// If next freepage doesn't exist
		while (1) {
			free_offset += PAGE_SIZE;
			read_page(table_id, (Page *)fp, PAGE_SIZE, free_offset);
			if (fp->next_page == 0 && hp->root_page != free_offset) {
				hp->free_page = free_offset;
				break;
			}
		}
	} else {
		// If next freepage exists
		hp->free_page = fp->next_page;
	}

	hp->num_pages++;
	mark_dirty(hb);
	release_pincount(hb);
	free(fp);
}

/* Make Buf structure
 */

Buf * make_buf(int table_id, int64_t offset) {
	Buf * hb;
	int buf_idx;
	header_page * hp;

	hb = get_buf(table_id, HEADERPAGE_OFFSET);
	hp = (header_page *)hb->page;

	buf_idx = get_free_buffer_index();
	// If free page assigned
	if (hp->free_page == offset) {
		alloc_freepage(table_id, hb, offset);
	} else {
		// If read page first
		read_page(table_id, buf[buf_idx].page, PAGE_SIZE, offset);
	}

	buf[buf_idx].page_offset = offset;
	buf[buf_idx].table_id = table_id;
	buf[buf_idx].is_dirty = false;
	if (update_LRU(&buf[buf_idx]) != 0) {
		//printf("make_buf(update_LRU)) error!!!\n");
	}

	release_pincount(hb);

	return &buf[buf_idx];
}

/* To get Buf structure of its offset
 * If page already exists, get Buf structure.
 * If not, assign Buf and read page of its offset.
 */
Buf * get_buf(int table_id, int64_t offset) {
	Buf * b;

	if ((b = find_buf(table_id, offset)) != NULL)
		return b;

	if (offset == 0) {
		return read_headerpage(table_id);	
	}
	// If page is first read, read page
	return make_buf(table_id, offset);
}

/* Open existing data file using ‘pathname’ or create one if not existed.
 * If success, return table_id. Otherwise, return -1.
 */
int open_table(char* pathname) {
	int fd;
	int table_id;
	header_page * hp;
	Buf * hb;

	table_id = atoi(&pathname[4]);

	// If file exists
	if (access(pathname, 0) == 0) {
		if (table[table_id] != 0)
			return table_id;
		if ((fd = open(pathname, O_RDWR | O_SYNC, 0644)) == -1) {
			// Fail to access
			return -1;
		} else {
			table[table_id] = fd;
			return table_id;
		}
	} else {
		// If file doesn't exist, make file and initialize header page and write into file.
		if ((fd = open(pathname, O_CREAT | O_RDWR | O_SYNC, 0644)) == -1) {
			// Fail to make file.
			return -1;
		} else {
			table[table_id] = fd;
			// Success to make file, initialize header page and write into file.
			hb = init_headerpage(table_id);
			hp = (header_page *)hb->page;
			hp->free_page = PAGE_SIZE;
			hp->root_page = 0;
			hp->num_pages = 1;	// header page
			// Make root page.
			// First root page is leaf page.
			Buf * b = get_buf(table_id, hp->free_page);
			leaf_page * root = (leaf_page *) b->page;
			root->parent_page = 0;
			root->is_leaf = 1;
			root->num_keys = 0;
			root->right_sibling = 0;

			hp->root_page = b->page_offset;

			hp->num_pages++;
			// write page into disk
			mark_dirty(b);
			mark_dirty(hb);

			release_pincount(b);
			release_pincount(hb);
			return table_id;
		}
	}
}
				
int close_table(int table_id) {
	LRU * cur;
	cur = LRU_list->head->next;
	while (cur != LRU_list->tail) {
		if (cur->buf->table_id == table_id) {
			Buf * vb;
			// Remove in LRU_list.
			cur->prev->next = cur->next;
			cur->next->prev = cur->prev;

			vb = cur->buf;

			if (vb->is_dirty && vb->page_offset != PAGE_NONE) {
				write_page(vb->table_id, vb->page, PAGE_SIZE, vb->page_offset);
			}

			vb->is_dirty = false;
			vb->in_LRU = false;
			vb->page_offset = PAGE_NONE;
			vb->pin_count = 0;

			LRU_list->num_lru--;
		}
		cur = cur->next;
	}
	close(table[table_id]);
	table[table_id] = 0;
	return 0;
}

int shutdown_db() {
	int i;
	LRU * cur;
	cur = LRU_list->head->next;
	while (cur != LRU_list->tail) {
		Buf * vb;
		if (!vb->in_LRU) {
			cur = cur->next;
			continue;
		}
		// Remove in LRU_list.
		cur->prev->next = cur->next;
		cur->next->prev = cur->prev;

		vb = cur->buf;

		if (vb->is_dirty && vb->page_offset != PAGE_NONE) {
			write_page(vb->table_id, vb->page, PAGE_SIZE, vb->page_offset);
		}

		vb->is_dirty = false;
		vb->in_LRU = false;
		vb->page_offset = PAGE_NONE;

		LRU_list->num_lru--;

		cur = cur->next;
	}

	for (i = 0; i < num_buf; i++) {
		free(buf[i].page);
		free(buf[i].lru);
	}
	free(buf);
	free(LRU_list);
	for (i = 0; i < LOG_BUFFER_SIZE; i++) {
		free(log_buf[i].header);
		free(log_buf[i].old_image);
		free(log_buf[i].new_image);
	}
	free(log_buf);

	for (i = 1; i < 11; i++) {
		close(i);
		table[i] = 0;
	}
	if (LRU_list->num_lru == 0) {
		return 0;
	}
	return -1;
}
