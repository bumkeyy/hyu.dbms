
// MAIN
/*
FILE* fp;

int main( int argc, char ** argv ) {

    char * input_file;
    FILE * fp;
    node * root;
    int input, range2;
    char instruction;
    char license_part;

    input_file = argv[2];
    fp = fopen(input_file, "r+");

    printf("> ");
    while (scanf("%c", &instruction) != EOF) {
        switch (instruction) {
        case 'd':
            scanf("%d", &input);
            root = delete(root, input);
            print_tree(root);
            break;
        case 'i':
            scanf("%d", &input);
            root = insert(root, input, input);
            print_tree(root);
            break;
        case 'f':
        case 'p':
            scanf("%d", &input);
            find_and_print(root, input, instruction == 'p');
            break;
        case 'r':
            scanf("%d %d", &input, &range2);
            if (input > range2) {
                int tmp = range2;
                range2 = input;
                input = tmp;
            }
            find_and_print_range(root, input, range2, instruction == 'p');
            break;
        case 'l':
            print_leaves(root);
            break;
        case 'q':
            while (getchar() != (int)'\n');
            return EXIT_SUCCESS;
            break;
        case 't':
            print_tree(root);
            break;
        case 'v':
            verbose_output = !verbose_output;
            break;
        case 'x':
            if (root)
                root = destroy_tree(root);
            print_tree(root);
            break;
        default:
            usage_2();
            break;
        }
        while (getchar() != (int)'\n');
        printf("> ");
    }
    printf("\n");

    return EXIT_SUCCESS;
}*/

#include "bpt.h"

#define NUM_COMMAND	10000

int main() {

	int i;
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
	for (i = 0; i < NUM_COMMAND; i++) {
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
	}
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
	




