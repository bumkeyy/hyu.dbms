/**
 *    @class Database System
 *    @file  bpt.h
 *    @brief Disk-based B+ tree with buffer 
 *    @author Kibeom Kwon (kgbum2222@gmail.com)
 *    @since 2017-11-14
 */

#ifndef __BPT_H__
#define __BPT_H__

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <fcntl.h>
#define false 0
#define true 1

#define LEAF_ORDER 32
#define INTERNAL_ORDER	249
//#define LEAF_ORDER 4
//#define INTERNAL_ORDER 4
#define PAGE_HEADER 128
#define HEADERPAGE_OFFSET 0
#define VALUE_SIZE	120
#define PAGE_SIZE	4096
#define BUF_SIZE	10000
#define TABLE_SIZE	11

// TYPES.

/* Type representing the records
 * to which a given key refers.
 * Rocord types are 2 types (in disk-based b+tree)
 * leaf_record and internal_record.
 * Leaf_record is containing key and value in Leaf page and
 * consisted of key (8byte) and value (120byte).
 * Internal_record is containing key and page pointer in
 * Internal page and consist of key (8byte) and one more page pointer.
 */
#pragma pack(push, 1)

typedef struct Page {
	char context[PAGE_SIZE];
} Page;

typedef struct LRU LRU;

typedef struct Buf {
	Page * page;
	int64_t table_id;	// table_id is (fd - 2).
	int64_t page_offset;
	bool is_dirty;		// When victim page is removed in LRU, if it is true, call write_page function.
	int pin_count;		// If it is in using, increment pin_count.
	bool in_LRU;		// If its page is in LRU, return true.
	LRU * lru;			// Each Buf structure has its LRU structure.
} Buf;

struct LRU {
	Buf * buf;
	struct LRU * prev;
	struct LRU * next;
};

// It LRU_list means buffer list.
// When reading the page, LRU structure of Page is located head of LRU_list.
typedef struct LRU_list {
	LRU * head;
	LRU * tail;
	int num_lru;
} LRU_LIST;

typedef struct leaf_record {
	int64_t key;
	char value[120];
} leaf_record;

typedef struct internal_record {
	int64_t key;
	int64_t page_offset;
} internal_record;

/* Type representing the pages.
 * There are 4 types of page. 
 * Header page is special and containing meta data.
 * Free page is maintained by free page list.
 * Leaf page is containing records.
 * Internal page is indexing internal or leaf page.
 * All page are 4096 bytes.
 */


// STRUCT

#pragma pack(push, 1)
typedef struct header_page {
	int64_t free_page;
	int64_t root_page;
	int64_t num_pages;
	int64_t reserved[509];
} header_page;

typedef struct free_page {
	int64_t  next_page;
	char unused[4088];
} free_page;

typedef struct leaf_page {
	int64_t parent_page;
	int is_leaf;
	int num_keys;
	int64_t reserved[13];
	int64_t right_sibling;
	leaf_record records[31];
} leaf_page;

typedef struct internal_page {
	int64_t parent_page;
	int is_leaf;
	int num_keys;
	int64_t reserved[13];
	int64_t one_more_page;
	internal_record records[248];
} internal_page;

#pragma pack(pop)

// GLOBALS

extern header_page * hp;
extern FILE* fp;
extern int fd;
extern Buf ** buf;
extern int num_buf;

// FUNCTION PROTOTYPES.

// OPEN AND INIT
int cut(int length);
int open_table(char* pathname);
void init_buf(int table_id);
Buf * get_buf(int table_id, int64_t offset);
Buf * find_buf(int table_id, int64_t offset);
Buf * make_buf(int table_id, int64_t offset);
void read_page(int table_id, Page * page, int64_t size, int64_t offset);
void write_page(int table_id, Page * page, int64_t size, int64_t offset);

// BUFFER POOL
int init_db(int num_buf);
int update_LRU(Buf * b);
void make_victim(void);
Buf * init_headerpage (int table_id);
void mark_dirty(Buf * b);
void release_pincount(Buf * b);
int close_table(int table_id);
int shutdown_db(void);

// FIND
Buf * find_leaf(int table_id, int64_t key);
char * find(int table_id, int64_t key);

// INSERT
int insert(int table_id, int64_t key, char * value);
int insert_into_leaf(Buf * b, int64_t key, char * value);
int insert_into_leaf_after_splitting(int table_id, Buf * b, int64_t key, char * value);
int insert_into_parent(int table_id, Buf * left_b, int64_t key, Buf * right_b);
int insert_into_new_root(int table_id, Buf * left_b, int64_t key, Buf * right_b);
int get_left_index(Buf * b, Buf * left_b);
int insert_into_internal(Buf * b, int left_index, int64_t key, Buf * right_b);
int insert_into_internal_after_splitting(int table_id, Buf * b, int left_index, int64_t key, Buf * right_b);

// DELETE
int delete(int table_id, int64_t key);
int delete_entry(int table_id, Buf * b, int64_t key);
Buf * remove_entry_from_page(Buf * b, int64_t key);
int adjust_root(int table_id, Buf * b);
int get_neighbor_index(int table_id, Buf * b);
int coalesce_pages (int table_id, Buf * b, Buf * nb, int neighbor_index, int64_t k_prime);
int redistribute_pages(int table_id, Buf * b, Buf * nb, int neighbor_index, int k_prime_index, int k_prime);




#endif /* __BPT_H__*/
