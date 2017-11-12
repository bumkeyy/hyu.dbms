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
	if (b->page_offset == 0) return NULL;
	for (i = 0; i < leaf->num_keys; i++) {
		if (leaf->records[i].key == key) break;
	}
	if (i == leaf->num_keys)
		return NULL;
	else {
		return (char *) leaf->records[i].value;
	}

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

// DELETE <KEY>

/* Utility function for deletion. Retrieves
 * the index of a page's nearest neighbor (silbing)
 * to the left if one exists. If no (the page is
 * the leftmost child), returns -1 to signify
 * this special case.
 */
int get_neighbor_index(Buf * b) {
	int i;
	internal_page * c, * parent;
	Buf * pb;
	c = (internal_page *)b->page;
	pb = get_buf(c->parent_page);
	parent = (internal_page *)pb->page;

	if (parent->one_more_page == b->page_offset)
		return -2;

	for (i = 0; i <= parent->num_keys; i++)
		if (parent->records[i].page_offset == b->page_offset)
			return i - 1;
}

Buf * remove_entry_from_page(Buf * b, int64_t key) {

	int i, num_pointers;
	internal_page * c;
	leaf_page * leaf;
	c = (internal_page *)b->page;

	if (c->is_leaf) {
		// If page is leaf page.
		leaf = (leaf_page *)b->page;

		// Remove the key and shift other keys accordingly.
		i = 0;
		while (leaf->records[i].key != key && i < leaf->num_keys) {
			i++;
		}
		for (++i; i < leaf->num_keys; i++) {
			leaf->records[i - 1].key = leaf->records[i].key;
			memcpy(leaf->records[i - 1].value, leaf->records[i].value, VALUE_SIZE);
		}
		leaf->num_keys--;
		write_page((Page *)leaf, PAGE_SIZE, b->page_offset);
	} else {
		// If page is internal page.
		i = 0;
		while (c->records[i].key != key)
			i++;
		for (++i; i < c->num_keys; i++) {
			c->records[i - 1].key = c->records[i].key;
			c->records[i - 1].page_offset = c->records[i].page_offset;
		}
		c->num_keys--;
		write_page((Page *)c, PAGE_SIZE, b->page_offset);
	}
	return b;
}

int adjust_root(Buf * b) {

	Buf * nb;
	internal_page * nroot;
	internal_page * root = (internal_page *)b->page;
	
	/* Case : nonempty root.
	 * Key and value have already been deleted,
	 * so nothing to be done.
	 */

	if (root->num_keys > 0)
		return 0;

	/* Case : empty root.
	 */

	// If it has a child, promote
	// the first (only) child
	// as the new root.
	
	if (!root->is_leaf) {
		hp->root_page = root->one_more_page;
		nb = get_buf(root->one_more_page);
		nroot = (internal_page *)nb->page;
		nroot->parent_page = 0;
		b->page_offset = 0;

		write_page((Page *)hp, PAGE_SIZE, 0);
		write_page((Page *)nroot, PAGE_SIZE, nb->page_offset);  
	}

	return 0;
}

/* Coalesces a page that has become
 * too small after deletion
 * with a neighboring page that
 * can accept the additional entries
 * without exceeding the maximum.
 */
int coalesce_pages (Buf * b, Buf * nb, int neighbor_index, int64_t k_prime) {

	int i, j, neighbor_insertion_index, n_end;
	Buf * temp, * child, * parent_b;
	internal_page * ci, *ni, * cp;
	leaf_page * cl, *nl;

	/* Swap neighbor with page if page is on the
	 * extreme left and neighbor is to its right.
	 */

	if (neighbor_index == -2) {
		temp = b;
		b = nb;
		nb = temp;
	}
	
	/* Starting point in the neighbor for copyunh
	 * keys and values from b.
	 * Recall that b and neighbor have swapped places
	 * in the special case of b being leftmost child.
	 */

	ni = (internal_page *) nb->page;
	parent_b = get_buf(ni->parent_page);
	
	neighbor_insertion_index = ni->num_keys;

	/* Case : nonleaf page.
	 * Append k_prime and the following page offset.
	 * Append all page offset and keys from the neighbor.
	 */

	if (!ci->is_leaf) {
		ci = (internal_page *)b->page;
		ni->records[neighbor_insertion_index].key = k_prime;
		ni->records[neighbor_insertion_index].page_offset = ci->one_more_page;

		n_end = ci->num_keys;

		for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
			ni->records[i].key = ci->records[j].key;
			ni->records[i].page_offset = ci->records[j].page_offset;
			ni->num_keys++;
			ci->num_keys--;
		}

		/* All children must now point up to the same parent.
		 */
		child = get_buf(ni->one_more_page);
		cp = (internal_page *)child->page;
		cp->parent_page = nb->page_offset;
		write_page((Page *)cp, PAGE_SIZE, child->page_offset);
		for (i = 0; i < ni->num_keys; i++ ) {
			child = get_buf(ni->records[i].page_offset);
			cp = (internal_page *)child->page;
			cp->parent_page = nb->page_offset;
			write_page((Page *)cp, PAGE_SIZE, child->page_offset);
		}

		ci->parent_page = 0;
		hp->num_pages--;
		write_page((Page *)hp, PAGE_SIZE, 0);
		write_page((Page *)ci, PAGE_SIZE, b->page_offset);
		write_page((Page *)ni, PAGE_SIZE, nb->page_offset);
		b->page_offset = 0;
	}

	/* In a leaf, append the keys and value of 
	 * b to nb.
	 */
	
	else {
		cl = (leaf_page *)b->page;
		nl = (leaf_page *)nb->page;
		for (i = neighbor_insertion_index, j = 0; j < cl->num_keys; i++, j++) {
			nl->records[i].key = cl->records[j].key;
			memcpy(nl->records[i].value, cl->records[j].value, VALUE_SIZE);
			nl->num_keys++;
		}
		nl->right_sibling = cl->right_sibling;

		cl->parent_page = 0;
		hp->num_pages--;
		write_page((Page *)hp, PAGE_SIZE, 0);
		write_page((Page *)cl, PAGE_SIZE, b->page_offset);
		write_page((Page *)nl, PAGE_SIZE, nb->page_offset);
		b->page_offset = 0;
	}

	return delete_entry(parent_b, k_prime);
}

/* Redistributes entries between two pages when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small page's entries without exceeding the
 * maximum.
 */

int redistribute_pages(Buf * b, Buf * nb, int neighbor_index, 
		int k_prime_index, int k_prime) {
	int i;
	internal_page * ci, * ni, * parent;
	leaf_page * cl, * nl;
	Buf * tmp, * pb;

	/* Case : b has a neighbor to the left.
	 * Pull the neighbor's last key-pointer pair over
	 * from the neighbor's right end to b's left end.
	 */

	ci = (internal_page *)b->page;
	pb = get_buf(ci->parent_page);
	parent = (internal_page *)pb->page;

	if (neighbor_index != -2) {
		if (ci->is_leaf) {
			// If page is leaf
			cl = (leaf_page *)b->page;
			nl = (leaf_page *)nb->page;

			for (i = cl->num_keys; i > 0; i--) {
				cl->records[i].key = cl->records[i - 1].key;
				memcpy(cl->records[i].value, cl->records[i - 1].value, VALUE_SIZE);
			}
			cl->records[0].key = nl->records[nl->num_keys - 1].key;
			memcpy(cl->records[0].value, nl->records[nl->num_keys - 1].value, VALUE_SIZE);
			parent->records[k_prime_index].key = cl->records[0].key;

			nl->records[nl->num_keys - 1].key = -1;
			cl->num_keys--;
			nl->num_keys++;

			write_page((Page *)cl, PAGE_SIZE, b->page_offset);
			write_page((Page *)nl, PAGE_SIZE, nb->page_offset);
			write_page((Page *)parent, PAGE_SIZE, pb->page_offset);
			
		} else {
			// If page is internal page
			ni = (internal_page *)nb->page;
			for (i = cl->num_keys; i > 0; i--) {
				ci->records[i].key = ci->records[i - 1].key;
				ci->records[i].page_offset = ci->records[i - 1].page_offset;
			}
			ci->records[0].key = k_prime;
			ci->records[0].page_offset = ci->one_more_page;
			ci->one_more_page = ni->records[ni->num_keys - 1].page_offset;
			parent->records[k_prime_index].key = ni->records[ni->num_keys - 1].key;

			ci->num_keys++;
			ni->num_keys--;

			write_page((Page *)ci, PAGE_SIZE, b->page_offset);
			write_page((Page *)ni, PAGE_SIZE, nb->page_offset);
			write_page((Page *)parent, PAGE_SIZE, pb->page_offset);	
		}
	}

	/* Case : b is the leftmost child.
	 * Take a key-value pair frome the neighbor to the right.
	 * Move the neighbor's leftmost key-value pair
	 * to b's rightmost position.
	 */
	else {
		if (ci->is_leaf) {
		// If page is leaf.
		cl = (leaf_page *)b->page;
		nl = (leaf_page *)nb->page;

		cl->records[cl->num_keys].key = nl->records[0].key;
		memcpy(cl->records[cl->num_keys].value, nl->records[0].value, VALUE_SIZE);
		parent->records[k_prime_index].key = nl->records[1].key;

		for (i = 0; i < nl->num_keys - 1; i++) {
			nl->records[i].key = nl->records[i + 1].key;
			memcpy(nl->records[i].value, nl->records[i + 1].value, VALUE_SIZE);
		}
		nl->num_keys--;
		cl->num_keys++;

		// Write to disk
		write_page((Page *)cl, PAGE_SIZE, b->page_offset);
		write_page((Page *)nl, PAGE_SIZE, nb->page_offset);
		write_page((Page *)parent, PAGE_SIZE, pb->page_offset);
		} else {
			ni = (internal_page *)nb->page;
			ci->records[ci->num_keys].key = k_prime;
			ci->records[ci->num_keys].page_offset = ni->one_more_page;
			parent->records[k_prime_index].key = ni->records[0].key;
			ni->one_more_page = ni->records[0].page_offset;

			for (i = 0; i < ni->num_keys - 1; i++) {
				ni->records[i].key = ni->records[i + 1].key;
				ni->records[i].page_offset = ni->records[i + 1].page_offset;
			}
			ni->num_keys--;
			ci->num_keys++;

			// Write to disk
			write_page((Page *)ci, PAGE_SIZE, b->page_offset);
			write_page((Page *)ni, PAGE_SIZE, nb->page_offset);
			write_page((Page *)parent, PAGE_SIZE, pb->page_offset);		
		}
	}
	return 0;
}



/* Deletes an entry from the B+ tree.
 * Removes the record and its key and value
 * from the leaf page, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */
int delete_entry(Buf * b, int64_t key) {
	int min_keys;
	Buf * nb, * pb;
	int neighbor_index;
	int64_t k_prime, k_prime_index, nb_offset;
	int capacity;
	internal_page * ipage, * parent, * neighbor;

	// Remove key and value from page.
	
	b = remove_entry_from_page(b, key);

	/* Case : deletion from the root.
	 */
	if (b->page_offset == hp->root_page)
		return adjust_root(b);

	/* Case : deletion from a page below the root.
	 */

	/* Determine minimum allowable size of page,
	 * to be preserved after deletion.
	 */

	ipage = (internal_page *) b->page;

	min_keys = ipage->is_leaf ? (LEAF_ORDER - 1) / 2 : (INTERNAL_ORDER - 1) / 2;

	/* Case : page stays at or above minimum.
	 * (The simple case.)
	 */

	if (ipage->num_keys >= min_keys)
		return 0;

	/* Case : page falls below minumum.
	 * Either coalescence or redistribution
	 * is needed.
	 */

	/* Find the appropriate neighbor page with which
	 * to coalesce.
	 * Also find the key (k_prime) in the parent
	 * between the pointer to page b and pointer
	 * to the neighbor.
	 */
	
	pb = get_buf(ipage->parent_page);
	parent = (internal_page *)pb->page;

	neighbor_index = get_neighbor_index(b);
	k_prime_index = neighbor_index == -2 ? 0 : neighbor_index + 1;
	k_prime = parent->records[k_prime_index].key;
	if (neighbor_index == -2) {
		nb_offset = parent->records[0].page_offset;
	} else if (neighbor_index == -1){
		nb_offset = parent->one_more_page;
	} else {
		nb_offset = parent->records[neighbor_index].page_offset;
	}

	nb = get_buf(nb_offset);
	neighbor = (internal_page *) nb->page;
	capacity = ipage->is_leaf ? LEAF_ORDER - 1 : INTERNAL_ORDER - 1;

	/* Coalescence. */

	if (neighbor->num_keys + ipage->num_keys <= capacity)
		return coalesce_pages(b, nb, neighbor_index, k_prime);

	/* Redistribution. */
	else
		return redistribute_pages(b, nb, neighbor_index, k_prime_index, k_prime);
}


/* Master deletion function.
 */
int delete(int64_t key) {

	Buf * b;
	
	if (find(key) == NULL) {
		printf("key : %ld doesn't exist.\n", key);
		return 0;
	}

	b = find_leaf(key);
	return delete_entry(b, key);
}

