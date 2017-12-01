#include "bpt.h"


int main(void) {
	int table = 0;
	int64_t key;
	char buf[255];
	char cmd = 0;
	char *tok;
	char * ret;

	init_db(4000);
	
	// no use buffer stdin, stdout
	setvbuf(stdin, NULL, _IONBF, 0);
	setvbuf(stdout, NULL, _IONBF, 0);


	while(1) {
        memset(buf, 0, 255);
		cmd = getchar();
		getchar();
		switch(cmd) {
			case 'o':
				if (fgets(buf, 255, stdin) != NULL) {
					buf[strlen(buf)-1] = '\0';
					printf("%d\n", open_table(buf));
				}
				break;
			case 'f': // find 
				if (fgets(buf, 255, stdin) != NULL) {
					tok = strtok(buf, " \n");
					table = atoi(buf);
					tok = strtok(NULL, " \n");
					key = atoll(tok);
					ret = find(table, key);
					if (ret == NULL)
						printf("Not Exists\n");
					else
						printf("Key: %" PRId64 ", Value: %s\n", key, ret);
					free(ret);	
				}							
				break;
			 case 'i': // insert
			 	if (fgets(buf, 255, stdin) != NULL) {
			 		tok = strtok(buf, " \n");
					table = atoi(tok);
					tok = strtok(NULL, " \n");
					key = atoll(tok);
					tok = strtok(NULL, " \n");
			 		insert(table, key, tok);
			 	}				
			 	break;	
			case 'j': // insert
			 	if (fgets(buf, 255, stdin) != NULL) {
			 		tok = strtok(buf, " \n");
					table = atoi(tok);
					tok = strtok(NULL, " \n");
					key = atoll(tok);
					tok = strtok(NULL, " \n");
			 		join_table(table, key, tok);
			 		printf("complete\n");
			 	}				
			 	break;	 	

            case 'd':
                if (fgets(buf, 30, stdin) != NULL) {
                    tok = strtok(buf, " \n");
					table = atoi(tok);
					tok = strtok(NULL, " \n");
					key = atoll(tok);
                    delete(table, key);
                }
                break;
			case 'c':
				if (fgets(buf, 30, stdin) != NULL) {
                    tok = strtok(buf, " \n");
					table = atoi(tok);
					close_table(table);
                }
				
				break;

			default:
				fprintf(stderr, "SHUTDOWN\n");
				shutdown_db();
				return 0;
		}
	}

	return 0;
}
