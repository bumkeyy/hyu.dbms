#ifndef __BPT_H__
#define __BPT_H__

// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#ifdef WINDOWS
#define bool char
#define false 0
#define true 1
#endif

// Default order is 4.
#define DEFAULT_ORDER 4

// Minimum order is necessarily 3.  We set the maximum
// order arbitrarily.  You may change the maximum order.
#define MIN_ORDER 3
#define MAX_ORDER 20

// Constants for printing part or all of the GPL license.
#define LICENSE_FILE "LICENSE.txt"
#define LICENSE_WARRANTEE 0
#define LICENSE_WARRANTEE_START 592
#define LICENSE_WARRANTEE_END 624
#define LICENSE_CONDITIONS 1
#define LICENSE_CONDITIONS_START 70
#define LICENSE_CONDITIONS_END 625

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
typedef struct leaf_record {
	int64_t key;
	char value[120];
} leaf_record;

typedef struct internal_record {
	int64_t key;
	int64_t page;
} internal_record;

/* Type representing the pages.
 * There are 4 types of page. 
 * Header page is special and containing meta data.
 * Free page is maintained by free page list.
 * Leaf page is containing records.
 * Internal page is indexing internal or leaf page.
 * All page are 4096 bytes.
 */

// TYPEDEF

typedef struct header_page header_page;
typedef struct free_page free_page;
typedef struct leaf_page leaf_page;
typedef struct internal_page internal_page;

// STRUCT

struct header_page {
	int64_t free_page;
	int64_t root_page;
	int64_t num_page;
	int64_t reserved[509];
}

struct free_page {
	int64_t next_page;
}

struct leaf_page {
	int64_t parent_page;
	int is_leaf;
	int num_keys;
	int64_t reserved[13];
	int64_t right_sibling;
	leaf_record records[31];
}

struct internal_page {
	int64_t parent_page;
	int is_leaf;
	int num_keys;
	int64_t reserved[13];
	int64_t one_more_page;
	internal_record records[248];
}

// GLOBALS

extern header_page* hp;
extern FILE* fp;

// FUNCTION PROTOTYPES.


// OPEN AND INIT

int open_db( char * pathname);
int64_t make_free_page();

// INSERTION
int insert( int64_t key, char * value);
int64_t find_leaf( int64_t key);
void insert_into_leaf(leaf_page* leaf, int64_t key, char* value);
int64_t insert_into_leaf_after_splitting(int64_t leaf_offset, int64_t key, char * value);
int64_t insert_into_parent(int64_t left_offset); 
int64_t start_new_tree(int64_t key, char * value);
int64_t get_left_index(int64_t parent_offset, int64_t left_offset);
int64_t insert_into_page(int64_t old_page_offset, int64_t left_index, int64_t key, int64_t right);
int64_t insert_into_page_after_splitting(int64_t old_page_offset, int64_t left_index, int64_t key, int64_t right)
int64_t insert_into_new_root(int64_t left_offset, int64_t right_offset, int64_t key);

// FIND
char * find( int64_t key);

// DELETION
int delete( int64_t key);
int delete_entry(int64_t offset, int64_t key);
int64_t remove_entry_from_page(int64_t offset, int64_t key);
int get_neighbor_index(int64_t offset);
int64_t coalesce_page(int64_t offset, int64_t neighbor_offset, int neighbor_index, int k_prime);





/******************Disk_based B+tree******************/

// TYPES.

/* Type representing the record
 * to which a given key refers.
 * In a real B+ tree system, the
 * record would hold data (in a database)
 * or a file (in an operating system)
 * or some other information.
 * Users can rewrite this part of the code
 * to change the type and content
 * of the value field.
 */
typedef struct record {
    int value;
} record;


/* Type representing a node in the B+ tree.
 * This type is general enough to serve for both
 * the leaf and the internal node.
 * The heart of the node is the array
 * of keys and the array of corresponding
 * pointers.  The relation between keys
 * and pointers differs between leaves and
 * internal nodes.  In a leaf, the index
 * of each key equals the index of its corresponding
 * pointer, with a maximum of order - 1 key-pointer
 * pairs.  The last pointer points to the
 * leaf to the right (or NULL in the case
 * of the rightmost leaf).
 * In an internal node, the first pointer
 * refers to lower nodes with keys less than
 * the smallest key in the keys array.  Then,
 * with indices i starting at 0, the pointer
 * at i + 1 points to the subtree with keys
 * greater than or equal to the key in this
 * node at index i.
 * The num_keys field is used to keep
 * track of the number of valid keys.
 * In an internal node, the number of valid
 * pointers is always num_keys + 1.
 * In a leaf, the number of valid pointers
 * to data is always num_keys.  The
 * last leaf pointer points to the next leaf.
 */
typedef struct node {
    void ** pointers;
    int * keys;
    struct node * parent;
    bool is_leaf;
    int num_keys;
    struct node * next; // Used for queue.
} node;

// GLOBALS.

/* The order determines the maximum and minimum
 * number of entries (keys and pointers) in any
 * node.  Every node has at most order - 1 keys and
 * at least (roughly speaking) half that number.
 * Every leaf has as many pointers to data as keys,
 * and every internal node has one more pointer
 * to a subtree than the number of keys.
 * This global variable is initialized to the
 * default value.
 */
extern int order;

/* The queue is used to print the tree in
 * level order, starting from the root
 * printing each entire rank on a separate
 * line, finishing with the leaves.
 */
extern node * queue;

/* The user can toggle on and off the "verbose"
 * property, which causes the pointer addresses
 * to be printed out in hexadecimal notation
 * next to their corresponding keys.
 */
extern bool verbose_output;


// FUNCTION PROTOTYPES.

// Output and utility.

void license_notice( void );
void print_license( int licence_part );
void usage_1( void );
void usage_2( void );
void usage_3( void );
void enqueue( node * new_node );
node * dequeue( void );
int height( node * root );
int path_to_root( node * root, node * child );
void print_leaves( node * root );
void print_tree( node * root );
void find_and_print(node * root, int key, bool verbose); 
void find_and_print_range(node * root, int range1, int range2, bool verbose); 
int find_range( node * root, int key_start, int key_end, bool verbose,
        int returned_keys[], void * returned_pointers[]); 
node * find_leaf( node * root, int key, bool verbose );
record * find( node * root, int key, bool verbose );
int cut( int length );

// Insertion.

record * make_record(int value);
node * make_node( void );
node * make_leaf( void );
int get_left_index(node * parent, node * left);
node * insert_into_leaf( node * leaf, int key, record * pointer );
node * insert_into_leaf_after_splitting(node * root, node * leaf, int key,
                                        record * pointer);
node * insert_into_node(node * root, node * parent, 
        int left_index, int key, node * right);
node * insert_into_node_after_splitting(node * root, node * parent,
                                        int left_index,
        int key, node * right);
node * insert_into_parent(node * root, node * left, int key, node * right);
node * insert_into_new_root(node * left, int key, node * right);
node * start_new_tree(int key, record * pointer);
node * insert( node * root, int key, int value );

// Deletion.

int get_neighbor_index( node * n );
node * adjust_root(node * root);
node * coalesce_nodes(node * root, node * n, node * neighbor,
                      int neighbor_index, int k_prime);
node * redistribute_nodes(node * root, node * n, node * neighbor,
                          int neighbor_index,
        int k_prime_index, int k_prime);
node * delete_entry( node * root, node * n, int key, void * pointer );
node * delete( node * root, int key );

void destroy_tree_nodes(node * root);
node * destroy_tree(node * root);

#endif /* __BPT_H__*/
