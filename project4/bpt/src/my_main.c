
// MAIN
#include "bpt.h"

#define NUM_COMMAND	30

int main() {

	int i, j;
	int num_buf = 10;
	int table_id1;
	int table_id2, table_id3;
	char string[120];
	char* file1 = "text1";
	char* file2 = "text2";
	char* result = "result";
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

	// find
	for (i = 0; i < NUM_COMMAND; i++) {
		if (find(table_id1 ,i) == NULL){
			printf("find(%d) fail!\n", i);
			continue;
		}
		strcpy(string, find(table_id1, i));
		printf("find(%d) : %s \n", i, string);
	}
	// close table
	if (close_table(table_id1) != 0) {
		printf("close_table() error!!!\n");
		return 0;
	}

	
	if((table_id2 = open_table(file2)) <= 0) {
		printf("open_db() error!!\n");
		return 0;
	}

	// Insert
	for (j = 0; j < NUM_COMMAND/2; j++) {
		i = 2*j;
		if (insert(table_id1, i, string_set[i%3])){
			printf("insert(%d) error!\n", i);
		}
		printf("insert(%d, %s)\n", i, string_set[i%3]);
	}
/*
	// insert
	
	for (i = 0; i < NUM_COMMAND; i++) {
		if (insert(table_id2, i, string_set[i%3])){
			printf("insert(%d) error!\n", i);
		}
		printf("insert(%d, %s)\n", i, string_set[i%3]);
	}

	// find
	for (i = 0; i < NUM_COMMAND; i++) {
		if (find(table_id2 ,i) == NULL){
			printf("find(%d) fail!\n", i);
			continue;
		}
		strcpy(string, find(table_id2, i));
		printf("find(%d) : %s \n", i, string);
	}*/
	if (close_table(table_id2) != 0) {
		printf("close_table() error!!!\n");
		return 0;
	}

	// open
	if((table_id1 = open_table(file1)) <= 0) {
		printf("open_db() error!!\n");
		return 0;
	}

	if((table_id2 = open_table(file2)) <= 0) {
		printf("open_db() error!!\n");
		return 0;
	}
	

/*
	// delete
	for (i = 0; i < NUM_COMMAND; i++) {
		if (delete(table_id1, i)){
			printf("delete(%d) error!\n", i);
			continue;
		}
		printf("delete(%d) \n", i);
	}	
*/
	if (join_table(table_id1, table_id2, result) != 0) {
		printf("join error!!\n");
	}
	return 0;
}
	




