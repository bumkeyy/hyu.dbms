#include "bpt.h"
int main(void) {

  int table_id;
  int size;
  int64_t input;
  char instruction;
  char buf[120];
  char path[120];
  init_db(16);

  while(scanf("%c", &instruction) != EOF){

    switch(instruction){
      case 'b':
        begin_transaction();
        break;
      case 'a':
        abort_transaction();
        break;
      case 'c':
        commit_transaction();
        break;
      case 'd':
        scanf("%d %ld", &table_id, &input);
        delete(table_id, input);

        break;

        case 'i':
          scanf("%d %ld %s", &table_id, &input, buf);
          insert(table_id, input, buf);
          break;

        case 'u':
          scanf("%d %ld %s", &table_id, &input, buf);
          update(table_id, input, buf);
          break;

        case 'f':
          scanf("%d %ld", &table_id, &input);
          char * ftest;
          if((ftest = find(table_id, input)) != NULL){
            printf("Key: %ld, Value: %s\n", input, ftest);
            fflush(stdout);
            free(ftest);
          }
          else{
            printf("Not Exists\n");
            fflush(stdout);
          }
          break;

        case 'n':
          scanf("%d", &size);
          init_db(size);
          break;

        case 'o':
          scanf("%s", path);
          table_id = open_table(path);
          // printf("%s table ID is: %d\n", path, table_id);
          break;

          case 'q':
            shutdown_db();
            while(getchar() != (int64_t)'\n');
            return 0;
            break;
      }

      while(getchar() != (int)'\n');

      // printf("> ");

}
printf("\n");
return 0;

}
