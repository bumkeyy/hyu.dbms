/*		@class Database System
 *		@file  join.h
 *		@brief join
 *		@author Kibeom Kwon (kgbum2222@gmail.com)
 *		@since 2017-11-23
 */

#include <pthread.h>
#include <bpt.h>


#define NUM_THREAD	4

void * (*ThreadFunc[4])(void * arg);


// FUNCTION
int join_table(int table_id_1, int table_id_2, char * pathname);
void * ThreadFunc_in_1(void * arg);
void * ThreadFunc_in_2(void * arg);
void * ThreadFunc_join(void * arg);
void * ThreadFunc_out(void * arg);




