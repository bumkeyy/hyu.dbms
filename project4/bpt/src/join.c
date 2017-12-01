/*		@class Database System
 *		@file  join.c
 *		@brief join
 *		@author Kibeom Kwon (kgbum2222@gmail.com)
 *		@since 2017-11-23
 */

#include "bpt.h"

// Find first leaf page.
Buf * get_first_leafpage(int table_id) {
	header_page * hp;
	Buf * hb, * b;
	internal_page * c;

	hb = get_buf(table_id, HEADERPAGE_OFFSET);
	hp = (header_page *) hb->page;

	b = get_buf(table_id, hp->root_page);
	c = (internal_page *) b->page;

	while (!c->is_leaf) {
		release_pincount(b);
		b = get_buf(table_id, c->one_more_page);
		c = (internal_page *) b->page;
	}

	release_pincount(hb);
	return b;
}

// Make output buffer.
// I use 1 buffer page to write result.
Buf * make_outbuffer() {
	int i;
	for (i = 0; i < num_buf; i++) {
		if (buf[i].pin_count == 0) {
			if (buf[i].is_dirty) {
				write_page(buf[i].table_id, buf[i].page, PAGE_SIZE, buf[i].page_offset);
				buf[i].page_offset = OUTPUT_OFFSET;
				buf[i].table_id = OUTPUT_BUFFER;
			}
			break;
		}
	}
	if (update_LRU(&buf[i]) != 0) {
		printf("make_outbuffer() error!!\n");
		return NULL;
	}
	return &buf[i];
}

// If key 1 == key 2,
// push result page to value.
// Check result page full.
int push_resultpage(FILE * fp, result_page * rp, leaf_page * l1, leaf_page * l2, int num_result, int num_key_1, int num_key_2) {

	if (num_result == JOIN_RESULT_SIZE) {
		while (num_result > 0) {
			int i = JOIN_RESULT_SIZE - num_result;
			fprintf(fp, "%" PRId64",%s,%" PRId64",%s\n", rp->value[i].key1, rp->value[i].value1, rp->value[i].key2, rp->value[i].value2); 
			num_result--;	
		}
	}

	rp->value[num_result].key1 = l1->records[num_key_1].key;
	memcpy(rp->value[num_result].value1, l1->records[num_key_1].value, VALUE_SIZE);
	rp->value[num_result].key2 = l2->records[num_key_2].key;
	memcpy(rp->value[num_result].value2, l2->records[num_key_2].value, VALUE_SIZE);
	return ++num_result;
}

// If reach end of file,
// have to flush result page to result file.
void flush_resultpage(FILE * fp, result_page * rp, int num_result) {
	//printf("flush to result page\n");

	int i = 0;
	while (i < num_result) {
			fprintf(fp, "%" PRId64",%s,%" PRId64",%s\n", rp->value[i].key1, rp->value[i].value1, rp->value[i].key2, rp->value[i].value2); 
			i++;	
		}
}

// Do natural join with given two tables and 
// write result table to the file using given pathname. 
// Return 0 if success, otherwise return non-zero value.
// Two tables should have been opened earlier.
int join_table(int table_id_1, int table_id_2, char * pathname) {
	
	leaf_page * leaf_1, * leaf_2;
	result_page * result;
	Buf * leaf_buf_1, * leaf_buf_2, * out_buf;
	int num_result, num_key_1, num_key_2, mark;
	int num_end1, num_end2;
	FILE * fp;

	fp = fopen(pathname, "w");

	// First leaf page of each file.
	leaf_buf_1 = get_first_leafpage(table_id_1);
	leaf_buf_2 = get_first_leafpage(table_id_2);

	leaf_1 = (leaf_page *) leaf_buf_1->page;
	leaf_2 = (leaf_page *) leaf_buf_2->page;

	// Set output buffer and result page.
	out_buf = make_outbuffer(); 
	result = (result_page *) out_buf->page;

	num_result = 0;
	num_key_1 = 0;
	num_key_2 = 0;
	num_end1 = leaf_1->num_keys;
	num_end2 = leaf_2->num_keys;

	// Compare key between table_id_1 and table_id_2.
	while (1) {
		while (leaf_1->records[num_key_1].key < leaf_2->records[num_key_2].key 
				&& num_key_1 < num_end1) 
			num_key_1++;
		while (leaf_1->records[num_key_1].key > leaf_2->records[num_key_2].key 
				&& num_key_2 < num_end2)
			num_key_2++;

		// Assert key1 == key2 or end of leaf page.
		// If search end of leaf page, read next leaf page.
		if (num_key_1 >= num_end1) {
			//printf("file 1 : next_leaf_page!!!\n");
			release_pincount(leaf_buf_1);

			// If current leaf page is end of file,
			// Flush result page,
			// Return 0.
			if (leaf_1->right_sibling == 0) {
				flush_resultpage(fp, result, num_result);
				return 0;
			} 

			// Go to next leaf page.
			leaf_buf_1 = get_buf(table_id_1, leaf_1->right_sibling);
			//printf("right_sibling : %lld \n",leaf_2->right_sibling); 
			leaf_1 = (leaf_page *) leaf_buf_1->page;

			num_key_1 = 0;
			num_end1 = leaf_1->num_keys;
		}

		// If search end of leaf page, read next leaf page.
		if (num_key_2 >= num_end2) {
			//printf("file 2 : next_leaf_page!!!\n");
			release_pincount(leaf_buf_2);

			// If current leaf page is end of file,
			// Flush result page,
			// Return 0.
			if (leaf_2->right_sibling == 0) {
				flush_resultpage(fp, result, num_result);
				return 0;
			} 

			// Go to next leaf page.
			leaf_buf_2 = get_buf(table_id_2, leaf_2->right_sibling);
			//printf("right_sibling : %lld \n",leaf_2->right_sibling); 
			leaf_2 = (leaf_page *) leaf_buf_2->page;

			num_key_2 = 0;
			num_end2 = leaf_2->num_keys;
		}

		// Save start of "block"
		mark = num_key_2;

		while (leaf_1->records[num_key_1].key == leaf_2->records[num_key_2].key && num_key_1 < num_end1) {
			// Outer loop over file 1.
			while (leaf_1->records[num_key_1].key == leaf_2->records[num_key_2].key && num_key_2 < num_end2) {
				// Inner loop over file 2.
				num_result = push_resultpage(fp, result, leaf_1, leaf_2, num_result, num_key_1, num_key_2); 
				num_key_2++;
			}
			num_key_2 = mark;
			num_key_1++;
		}
	}
	return 0;
}
