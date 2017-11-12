#include "bpt.h"

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


int main() {

	int i;
	char string[120];
	char* file = "a.db";
	char* string_set[3] = {
	"val1",
	"val2",
	"va13"
	};

	if(open_db(file)) {
		printf("open_db() error!!\n");
		return 0;
	}
	// insert
/*	
	for (i = 0; i < 50; i++) {
		if (insert(i, string_set[i%3])){
			printf("insert(%d) error!\n", i);
		}
		printf("insert(%d, %s)\n", i, string_set[i%3]);
	}
*/	

	// find
	for (i = 0; i < 10; i++) {
		if (find(i) == NULL){
			printf("find(%d) fail!\n", i);
			continue;
		}
		strcpy(string, find(i));
		printf("find(%d) : %s \n", i, string);
	}
	/*
	// delete
	for (i = 0; i < 50; i++) {
		if (delete(i)){
			printf("delete(%d) error!\n", i);
		}
		printf("delete(%d) \n", i);
	}*/
	return 0;
}
	




