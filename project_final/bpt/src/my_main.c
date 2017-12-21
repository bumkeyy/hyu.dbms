
// MAIN
#include "bpt.h"

#define NUM_COMMAND	10000

int main() {

	int i, j;
	int num_buf = 16;
	int table_id1;
	int table_id2, table_id3;
	char string[120];
	char* file1 = "DATA3";
	char* file2 = "table2.db";
	char* result = "my_result.txt";
	Buf * b;
	internal_page * page;
	char* string_set[3] = {
	"val1",
	"val2",
	"va13"
	};
	
	if (init_db(num_buf) != 0) {
		printf("init_db() error!\n");
		return 0;
	}
	
	if((table_id1 = open_table(file1)) <= 0) {
		printf("open_db() error!!\n");
		return 0;
	}	
/*
	b = get_buf(table_id1, 4096);
	page = (internal_page *)b->page;
	printf("num_keys : %d\n", page->num_keys);
	
	// Insert
	for (j = 0; j < NUM_COMMAND/2; j++) {
		i = 2*j;
		if (insert(table_id1, i, string_set[i%3])){
			printf("insert(%d) error!\n", i);
		}
		printf("insert(%d, %s)\n", i, string_set[i%3]);
	}
	// Insert
	for (j = 0; j < NUM_COMMAND/2; j++) {
		i = 2*j + 1;
		if (insert(table_id1, i, string_set[i%3])){
			printf("insert(%d) error!\n", i);
		}
		printf("insert(%d, %s)\n", i, string_set[i%3]);
	}
	
	close_table(table_id1);
	open_table(file1);

	begin_transaction();
	for (i = 0; i < 300; i++) {
	update(table_id1, i, "fuck");
	}
	commit_transaction();
	for (i = 290; i < 300; i++) {
		if (find(table_id1 ,i) == NULL){
			printf("find(%d) fail!\n", i);
			continue;
		}
		strcpy(string, find(table_id1, i));
		printf("find(%d) : %s \n", i, string);
	}

	begin_transaction();
	update(table_id1, 299, "Why");
	abort_transaction();
	begin_transaction();
	update(table_id1, 298, "GOOD");
	commit_transaction();
	begin_transaction();
	update(table_id1, 297, "NONONO");
	flush_log(end_num);*/
	for (i = 290; i < 300; i++) {
		if (find(table_id1 ,i) == NULL){
			printf("find(%d) fail!\n", i);
			continue;
		}
		strcpy(string, find(table_id1, i));
		printf("find(%d) : %s \n", i, string);
	}	/*
	exit(1);

	for (i = 0; i < NUM_COMMAND; i++) {
		if (find(table_id1 ,i) == NULL){
			printf("find(%d) fail!\n", i);
			continue;
		}
		strcpy(string, find(table_id1, i));
		printf("find(%d) : %s \n", i, string);
	}	
	// delete
	for (i = 0; i < NUM_COMMAND; i++) {
		if (delete(table_id1, i)){
			printf("delete(%d) error!\n", i);
			continue;
		}
		printf("delete(%d) \n", i);
	}*/
	shutdown_db();
	return 0;
}
	




