/**
 *    @class Database System
 *    @file  bpt.c
 *    @brief Diks-based B+ tree
 *    @author Kibeom Kwon (kgbum2222@gmail.com)
 *    @since 2017-11-04
 */       

#include "bpt.h"

// GLOBALS.

header_page * hp;
FILE * fp;
int fd;
Buf * buf;

// OPEN AND INIT BUF

// Read page from file
void read_page(Page * page, int64_t size, int64_t offset) {
	pread(fd, page, size, offset);
}

// Write page to file
void write_page(Page * page, int64_t size, int64_t offset) {
	pwrite(fd, page, size, offset);
}

/* Initialize buf array before open database file or make database file.
 * Using buf array make reusing page structure.
 */
void init_buf() {
	int i = 0;
	fd = 0;
	buf = (Buf *) malloc(sizeof(Buf) * BUF_SIZE);
	for (i = 0; i < BUF_SIZE; i++) {
		buf[i].page = (Page *)malloc(sizeof(Page));
		buf[i].page_offset = 0;
	}
}
	
/* Find Buf structure of its offset.
 * If exist, return buf pointer.
 * If not, return NULL
 */
Buf * find_buf(int64_t offset) {
	int buf_idx = offset / PAGE_SIZE;

	// If not exist.
	// 0 is always header page offset.
	if (buf[buf_idx].page_offset == 0)
		return NULL;

	return &buf[buf_idx];
}

/* Make Buf structure
 */
Buf * make_buf(int64_t offset) {
	free_page * fp;
	int buf_idx;
	int64_t free_offset;

	buf_idx = offset / PAGE_SIZE;
	free_offset = offset;

	// If free page assigned
	if (hp->free_page == offset) {
		fp = (free_page *)malloc(sizeof(free_page));
		read_page((Page *)fp, PAGE_SIZE, offset);

		if (fp->next_page == 0) {
		// If next freepage doesn't exist
			while (1) {
				free_offset += PAGE_SIZE;
				read_page((Page *)fp, PAGE_SIZE, free_offset);
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
		write_page((Page *)hp, PAGE_SIZE, 0);
		free(fp);

	} else {
	// If read page first
		read_page(buf[buf_idx].page, PAGE_SIZE, offset);
	}

	buf[buf_idx].page_offset = offset;

	return &buf[buf_idx];
}

/* To get Buf structure of its offset
 * if page already exists, get Buf structure.
 * If not, assign Buf and read page of its offset.
 */
Buf * get_buf(int64_t offset) {
	Buf * b;
	
	if (offset == 0)
		return NULL;
	
	if ((b = find_buf(offset)) != NULL)
		return b;

	// If page is first read, read page
	return make_buf(offset);
}

/* Open existing data file using ‘pathname’ or create one if not existed.
 * If success, return 0. Otherwise, return non-zero value.
 */
int open_db(char* pathname) {

	// If file already opens
	if (fd != 0) {
		close(fd);
	}
	 
	init_buf();

	// If file exists
	if (access(pathname, 0) == 0) {
		if ((fd = open(pathname, O_RDWR | O_SYNC, 0644)) == -1) {
			// Fail to access
			return -1;
		} else {
			// Success to access, read header page
			read_page((Page *)hp, PAGE_SIZE, 0);
			return 0;
		}
	} else {
		// If file doesn't exist, make file and initialize header page and write into file.
		if ((fd = open(pathname, O_CREAT | O_RDWR | O_SYNC, 0644)) == -1) {
			// Fail to make file.
			return -1;
		} else {

			hp = (header_page *)malloc(sizeof(header_page));
			// Success to make file, initialize header page and write into file.
			hp->free_page = PAGE_SIZE;
			hp->root_page = 0;
			hp->num_pages = 1;	// header page

			// Make root page. 
			// First root page is leaf page.
			Buf * b = get_buf(hp->free_page);
			leaf_page * root = (leaf_page *) b->page;
			root->parent_page = 0;
			root->is_leaf = 1;
			root->num_keys = 0;
			root->right_sibling = 0;

			hp->root_page = b->page_offset;

			hp->num_pages++;

			// write page into disk
			write_page((Page *)hp, PAGE_SIZE, 0);
			write_page((Page *)root, PAGE_SIZE, b->page_offset);

			return 0;
		}
	}
}

// FIND < KEY >

/* Find Buf pointer of leaf page which has key.
 */
Buf * find_leaf(int64_t key) {
	int i = 0;
	Buf * b = get_buf(hp->root_page);
	internal_page * c = (internal_page *) b->page;

	while (!c->is_leaf) {
		while (i < c->num_keys) {
			if (key >= c->records[i].key)
				i++;
			else
				break;
		}
		if (i == 0) {
			b = get_buf(c->one_more_page);
			c = (internal_page *) b->page;
		} else {
			b = get_buf(c->records[i - 1].page_offset);
			c = (internal_page *) b->page;
		}
	}
	return b;
}

/* Finds and returns the record to which
 * a key refers.
 */
char * find(int64_t key) {
	int i;
	Buf * b = find_leaf(key);
	leaf_page * leaf = (leaf_page *) b->page;
	if (leaf == NULL) return NULL;
	for (i = 0; i < leaf->num_keys; i++) {
		if (leaf->records[i].key == key) break;
	}
	if (i == leaf->num_keys)
		return NULL;
	else
		return (char *) leaf->records[i].value;
}

// INSERT <KEY> <VALUE>

/* Inserts a new value to a record and its corresponding
 * key into a leaf.
 * Returns 0.
 */
int insert_into_leaf(Buf * b, int64_t key, char * value) {

	int i, insertion_point;
	leaf_page * leaf = (leaf_page *) b->page;
	
	insertion_point = 0;
	while (insertion_point < leaf->num_keys && leaf->records[insertion_point].key < key)
		insertion_point++;

	for (i = leaf->num_keys; i > insertion_point; i--) {
		leaf->records[i].key = leaf->records[i - 1].key;
		memcpy(leaf->records[i].value, leaf->records[i - 1].value, VALUE_SIZE);
	}

	leaf->records[insertion_point].key = key;
	memcpy(leaf->records[insertion_point].value, value, VALUE_SIZE);
	leaf->num_keys++;

	// Write to disk.
	write_page((Page *) leaf, PAGE_SIZE, b->page_offset);

	return 0;
}
/* Inserts a new key and page offset
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
int insert_into_leaf_after_splitting(Buf * b, int64_t key, char * value) {
	
	Buf * new_b;
	int insertion_index, split, new_key, i, j;
	leaf_page * leaf, * new_leaf;
	int temp_keys[LEAF_ORDER];
	char temp_values[LEAF_ORDER][VALUE_SIZE];

	new_b = get_buf(hp->free_page);
	new_leaf = (leaf_page *)new_b->page;

	insertion_index = 0;
	leaf = (leaf_page *) b->page;

	while (insertion_index < LEAF_ORDER - 1 && leaf->records[insertion_index].key < key)
		insertion_index++;

	for (i = 0, j = 0; i < leaf->num_keys; i++, j++) {
		if (j == insertion_index) j++;
		temp_keys[j] = leaf->records[i].key;
		memcpy(temp_values[j], leaf->records[i].value, VALUE_SIZE);
	}

	temp_keys[insertion_index] = key;
	memcpy(temp_values[insertion_index], value, VALUE_SIZE);

	leaf->num_keys = 0;

	split = (LEAF_ORDER - 1) / 2 + 1;

	for (i = 0; i < split; i++) {
		memcpy(leaf->records[i].value, temp_values[i], VALUE_SIZE);
		leaf->records[i].key = temp_keys[i];
		leaf->num_keys++;
	}

	for (i = split, j = 0; i < LEAF_ORDER; i++, j++) {
		memcpy(new_leaf->records[j].value, temp_values[i], VALUE_SIZE);
		new_leaf->records[j].key = temp_keys[i];
		new_leaf->num_keys++;
	}

	leaf->right_sibling = new_b->page_offset;
	new_leaf->parent_page = leaf->parent_page;
	new_leaf->is_leaf = 1;
	new_key = new_leaf->records[0].key;

	// Write to disk
	write_page((Page *)leaf, PAGE_SIZE, b->page_offset);
	write_page((Page *)new_leaf, PAGE_SIZE, new_b->page_offset);

	return insert_into_parent(b, new_key, new_b);
}

/* Inserts a new key and page_offset into internal page
 * causing the page's size to exceed
 * the INTERNAL_OREDER, and causing the page to split into two.
 */
int insert_into_internal_after_splitting(Buf * b, int left_index, 
		int64_t key, Buf * right_b) {

	int i, j, split, k_prime;
	Buf * new_b, * child_b;
	internal_page * new_page, * child;
	internal_page * old_page, right;
	int64_t temp_keys[INTERNAL_ORDER];
	int64_t temp_pageoffset[INTERNAL_ORDER];

	old_page = (internal_page *)b->page;

	for (i = 0, j = 0; i < old_page->num_keys + 1; i++, j++) {
		if (j == left_index) j++;
		temp_pageoffset[j] = old_page->records[i].page_offset;
		temp_keys[j] = old_page->records[i].key;
	}
	temp_pageoffset[left_index] = right_b->page_offset;
	temp_keys[left_index] = key;

	split = (INTERNAL_ORDER - 1) / 2;

	new_b = get_buf(hp->free_page);
	new_page = (internal_page *)new_b->page;
	old_page->num_keys = 0;

	for (i = 0; i < split - 1; i++) {
		old_page->records[i].page_offset = temp_pageoffset[i];
		old_page->records[i].key = temp_keys[i];
		old_page->num_keys++;
	}

	new_page->one_more_page = temp_pageoffset[i];
	k_prime = temp_keys[split - 1];

	for (++i, j = 0; i < INTERNAL_ORDER; i++, j++) {
		new_page->records[j].page_offset = temp_pageoffset[i];
		new_page->records[j].key = temp_keys[i];
		new_page->num_keys++;
	}

	new_page->parent_page = old_page->parent_page;

	child_b = get_buf(new_page->one_more_page);
	child = (internal_page *)child_b->page;
	child->parent_page = new_b->page_offset;
	write_page((Page *)child, PAGE_SIZE, child_b->page_offset);

	for (i = 0; i < new_page->num_keys; i++) {
		child_b = get_buf(new_page->records[i].page_offset);
		child = (internal_page *)child_b->page;
		child->parent_page = new_b->page_offset;
		write_page((Page *)child, PAGE_SIZE, child_b->page_offset);
	}

	write_page((Page *) old_page, PAGE_SIZE, b->page_offset);
	write_page((Page *) new_page, PAGE_SIZE, new_b->page_offset);

	return insert_into_parent(b, k_prime, new_b); 
}

/* Helper funciton used in insert_into_parent
 * to find the index of the parent's key to 
 * the page to the left of the key to be inserted.
 */
int get_left_index(Buf * b, Buf * left_b) {
	
	internal_page * parent;
	int left_index = 0;
	parent = (internal_page *) b->page;

	while (left_index <= parent->num_keys &&
			parent->records[left_index].page_offset != left_b->page_offset)
		left_index++;
	return left_index + 1;
}

/* Inserts a new key and page offset to a internal page
 * into which these can fit
 * without violating the B+ tree properties.
 */
int insert_into_internal(Buf * b, int left_index, int64_t key, Buf * right_b) {
	int i;
	internal_page * parent;
	parent = (internal_page *)b->page;

	for (i = parent->num_keys; i > left_index; i--) {
		parent->records[i].page_offset = parent->records[i - 1].page_offset;
		parent->records[i].key = parent->records[i - 1].key;
	}
	parent->records[left_index].page_offset = right_b->page_offset;
	parent->records[left_index].key = key;
	parent->num_keys++;

	// Write to disk
	write_page((Page *)parent, PAGE_SIZE, b->page_offset);
	return 0;
}

/* Inserts a new key into the B+ tree.
 */
int insert_into_parent(Buf * left_b, int64_t key, Buf * right_b) {
	int left_index;
	internal_page * parent;
	internal_page * left;
	Buf * b;

	left = (internal_page *) left_b->page;
	
	b = get_buf(left->parent_page);

	/* Case : new root. */
	if (b == NULL)
		return insert_into_new_root(left_b, key, right_b);

	/* Case : leaf or internal page.
	 */

	/* Find the parent's pointer to the left page.
	 */

	left_index = get_left_index(b, left_b);

	/* Simple case : the new key fits into the page.
	 */

	parent = (internal_page *)b->page;
	if (parent->num_keys < INTERNAL_ORDER - 1)
		return insert_into_internal(b, left_index, key, right_b);

	/* Harder case : split a page in order
	 * to preserve the B+ tree properties.
	 */

	return insert_into_internal_after_splitting(b, left_index, key, right_b);
}

/* Creates a new root page for two subtrees
 * and inserts the appropriate key into
 * the new root page.
 */
int insert_into_new_root(Buf * left_b, int64_t key, Buf * right_b) {
	Buf * root_b;
	internal_page * root, * left, * right;

	root_b = get_buf(hp->free_page);
	root = (internal_page *)root_b->page;

	left = (internal_page *)left_b->page;
	right = (internal_page *)right_b->page;

	root->records[0].key = key;
	root->one_more_page = left_b->page_offset;
	root->records[0].page_offset = right_b->page_offset;
	root->num_keys++;
	root->parent_page = 0;
	left->parent_page = root_b->page_offset;
	right->parent_page = root_b->page_offset;
	hp->root_page = root_b->page_offset;

	// Write to disk
	write_page((Page *)root, PAGE_SIZE, root_b->page_offset);
	write_page((Page *)left, PAGE_SIZE, left_b->page_offset);
	write_page((Page *)right, PAGE_SIZE, right_b->page_offset);
	write_page((Page *)hp, PAGE_SIZE, 0);

	return 0;
}

/* Master insertion function.
 * Inserts a key and an associated value into
 * the B+ tree, causing the tree to be adjusted
 * however necessary to maintain the B+ tree
 * properties.
 */
int insert(int64_t key, char * value) {

	Buf * b;
	leaf_page * leaf;
	
	// If key is duplicate 
	if (find(key) != NULL) {
		printf("error : INSERT DUPLICATE KEY <%ld> !!!\n", key);
		return -1; 
	}

	/* Case : the tree already exists.
	 */

	b = find_leaf(key);
	leaf = (leaf_page *) b->page;

	/* Case : leaf has room for key.
	 */

	if (leaf->num_keys < LEAF_ORDER - 1) {
		return insert_into_leaf(b, key, value);
	}

	/* Case : leaf must be split.
	 */

	return insert_into_leaf_after_splitting(b, key, value);
}


