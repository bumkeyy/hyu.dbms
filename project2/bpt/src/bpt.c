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

int open_db(char * pathname) {
	// assign header page
	hp = (header_page *)malloc(sizeof(header_page));

	if ((fp = fopen(pathname, "r+")) == NULL) {
		fp = fopen(pathname, "w");
		fd = fileno(fp);
		hp->free_page = make_free_page();
		// write disk
		pwrite(fd, hp, PAGE_SIZE, 0);
	} else {
		fd = fileno(fp);
		pread(fd, hp, PAGE_SIZE, 0); 
	}
	return 0;
}
/* If make file and initialize Header page,
 * we have to make free_page.
 * Returns free_page pointer
 */
int64_t make_free_page() {	
	int i;
	int64_t * buffer;
	buffer = (int64_t*)malloc(sizeof(int64_t));

	// write disk
	fseek(fp, PAGE_SIZE, SEEK_SET);
	
	*buffer = 2 * PAGE_SIZE;
	fwrite(buffer, 8, 1, fp);	

	// make 20 of free pages
	for (i = 0; i < 30; i++) {
		*buffer += PAGE_SIZE;
		fseek(fp, 4088, SEEK_CUR);
		fwrite(buffer, 8, 1, fp);	
	}	
	fsync(fd);
	free(buffer);
	return PAGE_SIZE;
}

/* Traces the path from the root to a leaf, searching
 * by key.
 * Returns the leaf page containing the given key.
 */
int64_t find_leaf(int64_t key) {
	int i = 0;
	internal_page* c;
	int64_t offset;
	
	// first, c is root page
	c = (internal_page*)malloc(sizeof(internal_page));
	pread(fd, c, PAGE_SIZE, hp->root_page);

	if (hp->root_page == 0)
		return 0;

	while (!c->is_leaf) {
		while (i < c->num_keys) {
			if (c->num_keys == 1) break;
			if (key >= c->record[i].key) i++;
			else break;
		}
		// go to left page
		if (i == 0) {
			offset = c->one_more_page;
			pread(fd, c, PAGE_SIZE, c->one_more_page);
		} else {
			offset = c->record[i].value;
			pread(fd, c, PAGE_SIZE, c->record[i].value);
		}
	}
	free(c);
	return offset;
}

/* Inserts a new key and value
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
int64_t insert_into_leaf_after_splitting(int64_t leaf_offset, int64_t key, char * value) {

	leaf_page * new_leaf;
	leaf_page * leaf;
	int64_t * temp_keys;
	int64_t free_offset[1];
	char ** temp_values;
	int insertion_index, split, new_key, i, j;
	int64_t new_leaf_offset;

	leaf = (leaf_page*)malloc(sizeof(leaf_page));
	pread(fd, leaf, PAGE_SIZE, leaf_offset);
	new_leaf = (leaf_page *)malloc(sizeof(leaf_page));
	new_leaf->is_leaf = 1;

	temp_keys = (int64_t *)malloc(LEAF_ORDER*sizeof(int64_t));
	temp_values = (char **)malloc(LEAF_ORDER*sizeof(char *));
	for (i = 0; i < LEAF_ORDER; i++) 
		temp_values[i] = (char *)malloc(120*sizeof(char));

	insertion_index = 0;
	while (insertion_index < LEAF_ORDER - 1 && 
			leaf->record[insertion_index].key < key)
		insertion_index++;

	for (i = 0, j = 0; i < leaf->num_keys; i++, j++) {
		if (j == insertion_index) j++;
		temp_keys[j] = leaf->record[i].key;
		strcpy(temp_values[j], leaf->record[i].value);
	}

	temp_keys[insertion_index] = key;
	temp_values[insertion_index] = value;

	leaf->num_keys = 0;
	
	split = LEAF_ORDER / 2;

	for (i = 0; i < split; i++) {
		leaf->record[i].key = temp_keys[i];
		strcpy(leaf->record[i].value, temp_values[i]);
		leaf->num_keys++;
	}

	for (i = split, j = 0; i < LEAF_ORDER; i++, j++) {
		new_leaf->record[j].key = temp_keys[i];
		strcpy(new_leaf->record[j].value, temp_values[i]);
		new_leaf->num_keys++;
	}

	free(temp_keys);
	free(temp_values);

	new_leaf_offset = hp->free_page;
	leaf->right_sibling = new_leaf_offset;
	pread(fd, free_offset, 8, hp->free_page);
	hp->free_page = free_offset[0];
	
	/*
	for (i = leaf->num_keys; i < LEAF_ORDER - 1; i++)
		leaf->record[i].value = (char *)NULL;
	for (i = new_leaf->num_keys; i < LEAF_ORDER - 1; i++)
		new_leaf->record[i].value = (char *)NULL;
*/
	new_leaf->parent_page = leaf->parent_page;
	new_key = new_leaf->record[0].key;

	// write disk
	pwrite(fd, hp, PAGE_SIZE, 0);
	pwrite(fd, leaf, PAGE_SIZE, leaf_offset);
	pwrite(fd, new_leaf, PAGE_SIZE, new_leaf_offset);

	free(leaf);
	free(new_leaf);

	return insert_into_parent(leaf_offset, new_leaf_offset, new_key); 
}

/* Insert a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
int64_t insert_into_parent(int64_t left_offset, int64_t right_offset, int64_t key) {
	int64_t left_index;
	internal_page * parent;

	parent = (internal_page *)malloc(sizeof(internal_page));
	pread(fd, parent, PAGE_SIZE, left_offset);

	/* Case : new root.*/

	if (parent->parent_page == 0)
		return insert_into_new_root(left_offset, right_offset, key);


	/* Case : internal page
	 * Find the parent's pointer to the left 
	 * node.
	 */
	left_index = get_left_index(parent->parent_page, left_offset);

	/* Simple case : the new key fits into the page.
	 */

	if (parent->num_keys < INTERNAL_ORDER - 1)
		return insert_into_page(parent->parent_page, left_index, key, right_offset);

	/* Harder case : split a node in order
	 * to preserve the B+ tree properties.
	 */
	return insert_into_page_after_splitting(parent->parent_page, left_index, key, right_offset);	
}

/* Inserts a new key and value to a page
 * into a page, causing the page's size to exceed
 * the INTERNAL_ORDER, and causing the page to split into two.
 */
int64_t insert_into_page_after_splitting(int64_t old_page_offset, int64_t left_index,
		int64_t key, int64_t right) {

	int i, j, split;
	int64_t k_prime, k_value, child_offset, new_page_offset;
	internal_page * new_page, * child, * old_page;
	int64_t * temp_keys;
	int64_t * temp_values;
	int64_t free_offset[1];

	/* First create a temporary set of keys and values
	 * to hold everything in order, including
	 * the new key and value, inserted in their
	 * correct places.
	 * Then create a new page and copy half of the
	 * keys and values to the old page and
	 * the other half to the new.
	 */

	temp_values = (int64_t *)malloc((INTERNAL_ORDER) * sizeof(int64_t));
	temp_keys = (int64_t *)malloc((INTERNAL_ORDER) * sizeof(int64_t));
	old_page = (internal_page *)malloc(sizeof(internal_page));
	child = (internal_page *)malloc(sizeof(internal_page));
	// set old page
	pread(fd, old_page, PAGE_SIZE, old_page_offset);
	
	for (i = 0, j = 0; i < old_page->num_keys; i++, j++) {
		if (j == left_index) j++;
		temp_keys[j] = old_page->record[i].key;
	}

	for (i = 0, j = 0; i < old_page->num_keys; i++, j++) {
		if (j == left_index) j++;
		temp_values[j] = old_page->record[i].value;	
	}

	temp_keys[left_index] = key;
	temp_values[left_index] = right;

	/* Create the new page and copy
	 * half the keys and pointers to the
	 * old and half to the new.
	 */
	split = (INTERNAL_ORDER / 2) + 1;
	new_page = (internal_page *)malloc(sizeof(internal_page));
	old_page->num_keys = 1;		// has one more page
	for (i = 0; i < split; i++) {
		old_page->record[i].key = temp_keys[i];
		old_page->record[i].value = temp_values[i];
		old_page->num_keys++;
	}
	k_prime = temp_keys[i];
	k_value = temp_values[i];
	for (++i, j = 0; i < INTERNAL_ORDER; i++, j++) {
		new_page->record[j].value = temp_values[i];
		new_page->record[j].key = temp_keys[i];
		new_page->num_keys++;
	}
	// set new internal page
	new_page->parent_page = old_page->parent_page;
	new_page->one_more_page = k_value;
	new_page->num_keys++;
	new_page_offset = hp->free_page;
	pread(fd, free_offset, 8, hp->free_page);
	hp->free_page = free_offset[0];

	for (i = 0; i < new_page->num_keys; i++) {
		child_offset = new_page->record[i].value;
		pread(fd, child, PAGE_SIZE, child_offset);
		child->parent_page = new_page_offset;
		pwrite(fd, child, PAGE_SIZE, child_offset);
	}
	
	// write disk
	pwrite(fd, hp, PAGE_SIZE, 0);
	pwrite(fd, old_page, PAGE_SIZE, old_page_offset);
	pwrite(fd, new_page, PAGE_SIZE, new_page_offset);

	// free
	free(old_page);
	free(new_page);
	free(child);
	free(temp_keys);
	free(temp_values);
	
	return insert_into_parent(old_page_offset, new_page_offset, k_prime);
}


/* Inserts a enw key and value to a page
 * into a page into which these can fit
 * without violating the B+ tree properties.
 */
int64_t insert_into_page(int64_t parent_offset, int64_t left_index, 
		int64_t key, int64_t right) {
	int i;
	internal_page * parent;

	parent = (internal_page *)malloc(sizeof(internal_page));

	// set pages
	pread(fd, parent, PAGE_SIZE, parent_offset);

	for (i = parent->num_keys - 1; i > left_index; i--) {
		parent->record[i + 1].value = parent->record[i].value;
		parent->record[i + 1].key = parent->record[i].key; 
	}
	parent->record[left_index].value =right;
	parent->record[left_index].key = key;
	parent->num_keys++;
	
	// write disk
	pwrite(fd, parent, PAGE_SIZE, parent_offset);

	free(parent);
	return 1;
}


/* Helper function used in insert_into_parent
 * to find the index of the parent's pointer to 
 * the node to the left of the key to be inserted.
 */
int64_t get_left_index(int64_t parent_offset, int64_t left_offset) {
	int64_t left_index = 0;

	// set parent page
	internal_page * parent = (internal_page *)malloc(sizeof(internal_page));
	pread(fd, parent, PAGE_SIZE, parent_offset);

	while (left_index < parent->num_keys - 1 &&
			parent->record[left_index].value != left_offset)
		left_index++;

	free(parent);
	return left_index;
}


/* Inserts a new value to a record and its corresponding
 * key into a leaf.
 * Returns the altered leaf.
 */
void insert_into_leaf(int64_t leaf_offset, int64_t key, char * value) {

	int i, insertion_point;
	leaf_page * leaf;

	leaf = (leaf_page *)malloc(sizeof(leaf_page));
	pread(fd, leaf, PAGE_SIZE, leaf_offset);
	insertion_point = 0;

	while (insertion_point < leaf->num_keys && 
			leaf->record[insertion_point].key < key)
		insertion_point++;

	for (i = leaf->num_keys; i > insertion_point; i--) {
		leaf->record[i].key = leaf->record[i - 1].key;
		strcpy(leaf->record[i].value, leaf->record[i - 1].value);
	}
	leaf->record[insertion_point].key = key;
	strcpy(leaf->record[insertion_point].value, value);
	leaf->num_keys++;
	// write disk
	pwrite(fd, leaf, PAGE_SIZE, leaf_offset);
	// free
	free(leaf);
}

/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
int64_t insert_into_new_root(int64_t left_offset, int64_t right_offset, 
		int64_t key) {
	int64_t * root_offset;
	int64_t free_offset[1];
	internal_page * root = (internal_page *)malloc(sizeof(internal_page));
	root_offset = (int64_t *)malloc(sizeof(int64_t));
	root->record[0].key = key;
	root->one_more_page = left_offset;
	root->record[0].value = right_offset;
	root->num_keys = 2;
	root->parent_page = 0;

	*root_offset = hp->free_page;
	pread(fd, free_offset, 8, hp->free_page);
	hp->free_page = free_offset[0];
	hp->root_page = *root_offset;

	// write disk
	pwrite(fd, hp, PAGE_SIZE, 0);
	pwrite(fd, root_offset, 8, left_offset);
	pwrite(fd, root_offset, 8, right_offset);
	pwrite(fd, root, PAGE_SIZE, *root_offset);

	// free
	free(root_offset);
	free(root);

	return 0;
}

/* First insertion:
 * start a new tree.
 */
int64_t start_new_tree(int64_t key, char * value) {
	int64_t root_offset;
	int64_t leaf_offset;
	int64_t free_offset[1];
	internal_page * root;
	leaf_page * leaf;

	root = (internal_page *)malloc(sizeof(internal_page));
	leaf = (leaf_page *)malloc(sizeof(leaf_page));

	// assign free pages to root and leaf 
	root_offset = hp->free_page;
	pread(fd, free_offset, 8, hp->free_page);
	hp->free_page = free_offset[0];

	leaf_offset = hp->free_page;
	pread(fd, free_offset, 8, hp->free_page);
	hp->free_page = free_offset[0];

	// set root page
	hp->root_page = root_offset;
	root->parent_page = 0;
	root->is_leaf = 0;
	root->num_keys = 1;
	root->one_more_page = leaf_offset;

	// set leaf page
	leaf->parent_page = root_offset;
	leaf->is_leaf = 1;
	leaf->num_keys = 1;
	leaf->record[0].key = key;
	strcpy(leaf->record[0].value, value);

	// write disk
	pwrite(fd, hp, PAGE_SIZE, 0);
	pwrite(fd, root, PAGE_SIZE, root_offset);
	pwrite(fd, leaf, PAGE_SIZE, leaf_offset);

	free(root);
	free(leaf);

	return 0;
}

/* Master insertion function.
 * Inserts a key and an associated value into
 * the Disk-based B+ tree, causing the tree to be adjusted
 * however necessary to maintain the B+ tree
 * properties
 */
int insert(int64_t key, char * value) {
	
	int64_t leaf_offset;
	leaf_page* leaf = (leaf_page*)malloc(sizeof(leaf_page));

	/* The current implementation ignores
	 * duplicates.
	 */
	if (find(key) != NULL)
		return 0;

	/* Case : the tree does not exist yet.
	 * Start a new tree.
	 */
	if (hp->root_page == 0)
		return start_new_tree(key, value);

	/* Case : the tree already exists.
	 * (Rest of functuin body.)
	 */

	leaf_offset = find_leaf(key);

	// read disk and write leaf_page in memory
	pread(fd, leaf, PAGE_SIZE, leaf_offset);

	/* Case : leaf has room for key and value
	 */
	if (leaf->num_keys < LEAF_ORDER - 1) {
		free(leaf);
		insert_into_leaf(leaf_offset, key, value);
		return 0;
	}

	/* Case : leaf must be split
	 */

	free(leaf);
	return insert_into_leaf_after_splitting(leaf_offset, key, value);
}

/* Finds and returns the record to which
 * a key refers.
 */
char * find(int64_t key) {
	int i = 0;
	int64_t leaf_offset;
	leaf_page * leaf = (leaf_page *)malloc(sizeof(leaf_page));
	leaf_offset = find_leaf(key);
	if (leaf_offset == 0) return NULL;
	pread(fd, leaf, PAGE_SIZE, leaf_offset);
	for (i = 0; i < leaf->num_keys; i++)
		if (leaf->record[i].key == key) break;
	if (i == leaf->num_keys)
		return NULL;
	else
		return (char *)leaf->record[i].value;
}


int64_t remove_entry_from_page(int64_t offset, int64_t key) {

	int i, num_values;
	leaf_page * leaf;
	internal_page * page;
	
	// Set leaf page
	leaf = (leaf_page *)malloc(sizeof(leaf_page));
	pread(fd, leaf, PAGE_SIZE, offset);

	if (leaf->is_leaf) {
		// Remove the key and shift other keys accordingly.
		i = 0;
		while (leaf->record[i].key != key) 
			i++;
		for (++i; i < leaf->num_keys; i++) {	
			strcpy(leaf->record[i - 1].value, leaf->record[i].value);
			leaf->record[i - 1].key = leaf->record[i].key;
		}

		// One key fewer.
		leaf->num_keys--;

		// write disk
		pwrite(fd, leaf, PAGE_SIZE, offset);
	
	} else {
		page = (internal_page *)malloc(sizeof(internal_page));
		pwrite(fd, page, PAGE_SIZE, offset);

		// Remove the key and shift other keys accordingly.
		i = 0;
		while (page->record[i].key != key) 
			i++;
		for (++i; i < page->num_keys - 1; i++) {	
			page->record[i - 1].key = page->record[i].key;
			page->record[i - 1].value = page->record[i].value;
		}

		// One key fewer.
		page->num_keys--;

		// write disk
		pwrite(fd, page, PAGE_SIZE, offset);

		free(page);
	}

	free(leaf);
	return offset;
}

/* Utility function for deletion. Retrieves
 * the index of a page's nearest neighbor(sibling)
 * to the left if one exists. If not (the page
 * is the leftmost child), return -2, -1 to signify
 * this special case.
 */
int get_neighbor_index(int64_t offset) {
	
	int i;
	internal_page * parent;
	int64_t parent_offset[1];
	parent = (internal_page *)malloc(sizeof(internal_page));

	// set parent page
	pread(fd, parent_offset, 8, offset);
	pread(fd, parent, PAGE_SIZE, parent_offset[0]);
	
	/* Return the index of the key to the left
	 * of the page offset in the parent pointing
	 * to page.
	 * If page is the leftmost child, this means
	 * return -2.
	 * If page is the first key page, this means
	 * return -1.
	 */
	if (parent->one_more_page == offset) {
		// free
		free(parent);
		return -2;
	}
	for (i = 0; i < parent->num_keys; i++) 
		if (parent->record[i].value == offset) {
			// free
			free(parent);
			return i - 1;
		}
}

/* Coalesces a page that has become
 * too small after deletion
 * with a neighboring page that
 * can accept the additional entries
 * without exceeding the maximum.
 */
int64_t coalesce_page(int64_t offset, int64_t neighbor_offset, int neighbor_index,
		int k_prime) {
	int i, j, neighbor_insertion_index, n_end;
	int64_t tmp;
	int64_t buffer[1];
	internal_page * neighbor, * page;
	leaf_page* n, * leaf;

	/* Swap neighbor with page if page is on the
	 * extreme left and neighbor is to its right.
	 */

	if (neighbor_index == -2) {
		tmp = offset;
		offset = neighbor_offset;
		neighbor_offset = tmp;
	}

	/* Starting point in the neighbor for copying
	 * keys and values from page.
	 * Recall that page and neighbor have swapped places
	 * in the special case of page being a leftmost child.
	 */

	// set page
	page = (internal_page *)malloc(sizeof(internal_page));
	neighbor = (internal_page *)malloc(sizeof(internal_page));
	leaf = (leaf_page *)malloc(sizeof(leaf_page));
	n = (leaf_page *)malloc(sizeof(leaf_page));
	pread(fd, page, PAGE_SIZE, offset);
	pread(fd, neighbor, PAGE_SIZE, neighbor_offset);

	neighbor_insertion_index = neighbor->num_keys - 1;

	/* Case : nonleaf page.
	 * Append k_prime and the following pointer.
	 * Append all pointers and keys from the neighbor.
	 */

	if (!page->is_leaf) {

		/* Append k_prime.
		 */
		neighbor->record[neighbor_insertion_index].key = k_prime;
		neighbor->record[neighbor_insertion_index].value = page->one_more_page;

		n_end = page->num_keys;

		for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
			neighbor->record[i].key = page->record[j].key;
			neighbor->record[i].value = page->record[j].value;
			neighbor->num_keys++;
			page->num_keys--;
		}

		// write disk
		pwrite(fd, page, PAGE_SIZE, offset);
		pwrite(fd, neighbor, PAGE_SIZE, neighbor_offset);

		/* All children must now point up to the same parent.
		 */

		for (i = 0; i < neighbor->num_keys; i++) {
			buffer[0] = neighbor_offset;
			pwrite(fd, buffer, 8, neighbor->record[i].value);
		}	

		tmp = neighbor->parent_page;
	}
	
	/* In a leaf page, append the keys and values of
	 * n to the leaf.
	 * Set the leaf's first pointer to point to
	 * what had been n's right neighbor.
	 */

	else {
		// set leaf pages
		pread(fd, n, PAGE_SIZE, offset);
		pread(fd, leaf, PAGE_SIZE, neighbor_offset);

		neighbor_insertion_index = leaf->num_keys;

		for (i = neighbor_insertion_index, j  = 0; j < n->num_keys; i++, j++) {
			leaf->record[i].key = n->record[j].key;
			strcpy(leaf->record[i].value, n->record[j].value);
			leaf->num_keys++;
		}
		leaf->right_sibling = n->right_sibling;

		// write disk
		pwrite(fd, n, PAGE_SIZE, offset);
		pwrite(fd, leaf, PAGE_SIZE, neighbor_offset);

		tmp = leaf->parent_page;
	}

	// free
	free(page);
	free(neighbor);
	free(leaf);
	free(n);

	return delete_entry(tmp, k_prime);
}

/* Redistributes entries between 2 page when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small page's entries without exceeding the
 * maximum
 */
int redistribute_page(int64_t offset, int64_t neighbor_offset, int neighbor_index,
		int k_prime_index, int k_prime) {

	int i;
	int64_t key[1];
	leaf_page * leaf, * n;
	internal_page * page, * neighbor, * parent;
	
	/* Case : page has a neighbor to the left.
	 * Pull hte neighbor's last key and pointer pair over
	 * from the neighbor's right end to page's left end.
	 */

	leaf = (leaf_page *)malloc(sizeof(leaf_page));
	n = (leaf_page *)malloc(sizeof(leaf_page));
	page = (internal_page *)malloc(sizeof(internal_page));
	neighbor = (internal_page *)malloc(sizeof(internal_page));
	parent = (internal_page *)malloc(sizeof(internal_page));

	// Set pages
	pread(fd, n, PAGE_SIZE, offset);
	pread(fd, leaf, PAGE_SIZE, neighbor_offset);
	pread(fd, page, PAGE_SIZE, offset);
	pread(fd, neighbor, PAGE_SIZE, neighbor_offset);
	pread(fd, parent, PAGE_SIZE, n->parent_page);

	if (neighbor_index != -2) {
		if (!n->is_leaf) {
			for (i = page->num_keys - 1; i > 0; i--) {
				page->record[i].key = page->record[i - 1].key;
				page->record[i].value = page->record[i - 1].value;
			}  
			pread(fd, key, 8, page->one_more_page + 128);
			page->record[0].key = key[0];
			page->record[0].value = page->one_more_page;	
			
			page->one_more_page = neighbor->record[neighbor->num_keys - 2].value;
			neighbor->record[neighbor->num_keys - 2].key = 0;
			neighbor->record[neighbor->num_keys - 2].value = 0;
			pwrite(fd, key, 8, page->one_more_page);
		} else {
			for (i = n->num_keys; i > 0; i--) {
				n->record[i].key = n->record[i - 1].key;
				strcpy(n->record[i].value, n->record[i - 1].value);
			}
			n->record[0].key = leaf->record[leaf->num_keys - 1].key;
			strcpy(n->record[0].value, leaf->record[leaf->num_keys - 1].value);
			parent->record[k_prime_index].key = n->record[0].key;
			// write disk
			pwrite(fd, parent, PAGE_SIZE, n->parent_page);
		}
	}

	/* Case : n is the leftmost child.
	 * Take a key, pointer pair from the neighbor to the right.
	 * Move the neighbor's leftmost key_pointer pair
	 * to n's rightmost position.
	 */

	else {
		if (n->is_leaf) {
			n->record[n->num_keys].key = leaf->record[0].key;
			strcpy(n->record[n->num_keys].value, leaf->record[0].value);
			parent->record[k_prime_index].key = leaf->record[1].key;	
			for (i = 0; i < leaf->num_keys - 1; i++) {
				leaf->record[i].key = leaf->record[i + 1].key;
				strcpy(leaf->record[i].value, leaf->record[i].value);
			}
		}
		else {
			pread(fd, key, 8, neighbor->one_more_page + 128);
			page->record[page->num_keys - 1].key = key[0];
			page->record[page->num_keys - 1].value = neighbor->one_more_page;
			neighbor->one_more_page = neighbor->record[0].value;
			parent->record[k_prime_index].key = neighbor->record[0].key;
			for (i = 0; i < neighbor->num_keys - 2; i++) {
				neighbor->record[i].key = neighbor->record[i + 1].key;	
				neighbor->record[i].value = neighbor->record[i + 1].value;
			}
			key[0] = offset;
			pwrite(fd, key, 8, page->record[page->num_keys - 1].value);
		}	
	}
	n->num_keys++;
	leaf->num_keys--;
	page->num_keys++;
	neighbor->num_keys--;

	if (n->is_leaf) {
		pwrite(fd, n, PAGE_SIZE, offset);
		pwrite(fd, leaf, PAGE_SIZE, neighbor_offset);
	}else {
		pwrite(fd, page, PAGE_SIZE, offset);
		pwrite(fd, neighbor, PAGE_SIZE, neighbor_offset); 
	}
	pwrite(fd, parent, PAGE_SIZE, n->parent_page);

	// free
	free(n);
	free(leaf);
	free(page);
	free(neighbor);
	free(page);
	return 0;
}

int adjust_root(int64_t offset) {	
	internal_page * root;

	root = (internal_page *)malloc(sizeof(internal_page));
	pread(fd, root, PAGE_SIZE, offset);

	/* Case : nonempty root.
	 * Key and pointer have already been deleted,
	 * so nothing to be done.
	 */
	if (root->num_keys > 0)
		return 0;
	
	
	return 1;
}

/* Deletes an entry from the B+ tree.
 * Removes the record and its key and value
 * from the leaf page, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */
int delete_entry(int64_t offset, int64_t key) {
	
	int64_t min_keys;
	leaf_page * leaf;
	internal_page * page;
	leaf_page * neighbor;
	internal_page * parent;
	int64_t neighbor_offset;
	int k_prime_index, k_prime, neighbor_index;
	int capacity;

	// Remove key and value from leaf page.
	
	offset = remove_entry_from_page(offset, key);

	/* Case : deletion from the root.
	 */

	if (offset == hp->root_page)
		return adjust_root(offset);

	/* Case : deletion from a page below the root
	 * (Rest of function body.)
	 */

	/* Determine minimum allowable size of page,
	 * to be perserved after deletion.
	 */

	// set page
	page = (internal_page *)malloc(sizeof(internal_page));
	pread(fd, page, PAGE_SIZE, offset);

	min_keys = page->is_leaf ? (LEAF_ORDER - 1) / 2 + 1 : (INTERNAL_ORDER / 2) - 1;

	/* Case : page stays at or above minimum.
	 * (The simple case.)
	 */

	if (page->num_keys >= min_keys)
		return 0;

	/* Case : page falls below minimum.
	 * Either coalescenc or redistribution
	 * is needed.
	 */

	/* Find the appropriate neighbor page with which
	 * to coalesce.
	 * Also find the key (k_prime) in parent
	 * between the pointer to page n and the pointer
	 * to the neighbor.
	 */

	// set parent page
	parent = (internal_page *)malloc(sizeof(internal_page));
	pread(fd, parent, PAGE_SIZE, leaf->parent_page);

	neighbor_index = get_neighbor_index(offset);
	if (neighbor_index == -2) {
		k_prime = parent->record[0].key;
		neighbor_offset = parent->record[0].value;
	}else if (neighbor_index == -1) {
		k_prime = parent->record[0].key;
		neighbor_offset = parent->one_more_page;
	}else {
		k_prime_index = neighbor_index;
		k_prime = parent->record[neighbor_index].key;
		neighbor_offset = parent->record[neighbor_index].value;
	}
	
	// set neighbor page
	neighbor = (leaf_page *)malloc(sizeof(leaf_page));
	pread(fd, neighbor, PAGE_SIZE, neighbor_offset);

	capacity = page->is_leaf ? LEAF_ORDER : INTERNAL_ORDER;

	/* Coalescence. */

	if (neighbor->num_keys + page->num_keys < capacity) {
		//free
		free(page);
		free(parent);
		free(neighbor);
		return coalesce_page(offset, neighbor_offset, neighbor_index, k_prime);
	}

	/* Redistribution */
	else {
		free(page);
		free(parent);
		free(neighbor);
		return redistribute_page(offset, neighbor_offset, neighbor_index,
				k_prime_index, k_prime);

	}
}

/* Master deletion fuction
 */
int delete(int64_t key) {
	int64_t key_offset;
	char key_value[120];

	key_offset = find_leaf(key);
	strcpy(key_value, find(key));
	if (key_offset != 0 || key_value != NULL) {
		return delete_entry(key_offset, key);
	}
	return 0;
}


