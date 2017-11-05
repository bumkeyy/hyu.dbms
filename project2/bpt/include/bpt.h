/**
 *    @class Database System
 *    @file  bpt.h
 *    @brief Diks-based B+ tree
 *    @author Kibeom Kwon (kgbum2222@gmail.com)
 *    @since 2017-11-04
 */

#ifndef __BPT_H__
#define __BPT_H__

// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#define bool char
#define false 0
#define true 1

/******************Disk_based B+tree******************/
#define LEAF_ORDER 32
#define INTERNAL_ORDER	249
#define PAGE_HEADER 128
#define VALUE	120
#define PAGE_SIZE	4096

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
typedef struct leaf_record {
	int64_t key;
	char value[120];
} leaf_record;

typedef struct internal_record {
	int64_t key;
	int64_t value;
} internal_record;
#pragma pack(pop)

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
struct header_page {
	int64_t free_page;
	int64_t root_page;
	int64_t num_page;
	int64_t reserved[509];
};

struct free_page {
	int64_t next_page;
};

struct leaf_page {
	int64_t parent_page;
	int is_leaf;
	int num_keys;
	int64_t reserved[13];
	int64_t right_sibling;
	leaf_record record[31];
};

struct internal_page {
	int64_t parent_page;
	int is_leaf;
	int num_keys;
	int64_t reserved[13];
	int64_t one_more_page;
	internal_record record[248];
};
#pragma pack(pop)

// TYPEDEF

typedef struct header_page header_page;
typedef struct free_page free_page;
typedef struct leaf_page leaf_page;
typedef struct internal_page internal_page;

// GLOBALS

extern header_page * hp;
extern FILE* fp;
extern int fd;

// FUNCTION PROTOTYPES.


// OPEN AND INIT

int open_db( char * pathname);
int64_t make_free_page();

// INSERTION
int insert( int64_t key, char * value);
int64_t find_leaf( int64_t key);
void insert_into_leaf(int64_t leaf, int64_t key, char* value);
int64_t insert_into_leaf_after_splitting(int64_t leaf_offset, int64_t key, char * value);
int64_t insert_into_parent(int64_t left_offset, int64_t right_offset, int64_t key); 
int64_t start_new_tree(int64_t key, char * value);
int64_t get_left_index(int64_t parent_offset, int64_t left_offset);
int64_t insert_into_page(int64_t old_page_offset, int64_t left_index, int64_t key, int64_t right);
int64_t insert_into_page_after_splitting(int64_t old_page_offset, int64_t left_index, int64_t key, int64_t right);
int64_t insert_into_new_root(int64_t left_offset, int64_t right_offset, int64_t key);

// FIND
char * find( int64_t key);

// DELETION
int delete( int64_t key);
int delete_entry(int64_t offset, int64_t key);
int64_t remove_entry_from_page(int64_t offset, int64_t key);
int get_neighbor_index(int64_t offset);
int64_t coalesce_page(int64_t offset, int64_t neighbor_offset, int neighbor_index, int k_prime);
int redistribute_page(int64_t offset, int64_t neighbor_offset, int neighbor_index, int k_prime_index, int k_prime);
int adjust_root(int64_t offset);



/******************Disk_based B+tree******************/

#endif /* __BPT_H__*/
