#include <stdlib.h> 
#include <stdio.h>
#include <string.h> 
#include "mpi.h"

#define C_MAX_NUM_LINES 32000 
#define C_MAX_LINE_WIDTH 512 


int main(){

    int maxNum; 
    int comm_size; /*Number of processes*/
    int my_rank; /*Process id number*/ 

    /*Initialize the MPI environment*/
    MPI_Init(&argc, &argv);

    /**Get the number of processes*/
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size); 

    /*Get the rank of the process*/
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    int pLineStartIndex[C_MAX_NUM_LINES]; //keep track of which line start where
    char pszFileBuffer[C_MAX_NUM_LINES * C_MAX_LINE_WIDTH]; //table
    int nTotalLines = 0;

    if(my_rank == 0){
        //Get file one name 

        char szLine[C_MAX_LINE_WIDTH];

        FILE *f = fopen("filename.txt", "rt"); 

        //If cannot open file
        if(f==NULL){
            fprintf(stderr, "Cannot open file\n");
            exit(EXIT_FAILURE);
        }

        pLineStartIndex[0] = 0;
        while(fgets(szLine, C_MAX_LINE_WIDTH, f)!= NULL){
            strcpy(pszFileBuffer + pLineStartIndex[nTotalLines], szLine);

            int length = strlen(szLine); 
            nTotalLines++;
            pLineStartIndex[nTotalLines] = pLineStartIndex[nTotalLines-1] 
                                            + length + 1;

        }
        fclose(f);

    }    

    char *key = "bac";

    int a = djb2(key);
    int b = sdbm(key);
    printf("%d, %d", a, b);

    return 0;
}


        #pragma omp Parallel{
            int id = omp_get_thread_num();
            int num_threads = omp_get_num_threads();

            /*
            int portion = total/nthreads;
            int startNum = id * portion;
            int endNum = (id + 1) * portion;
            */

           
        }


/*
        for(int i=0; i < table_len-1; i++){
            char dest[1024];
            memset(dest, '\0', sizeof(dest));
            strncpy(dest, (t1 + line_start_index[i]), (line_start_index[i+1]-line_start_index[i]));

            char line[1024];
            memset(line, '\0', sizeof(line));
            strcpy(line, dest);
            
            char *join_colum = split_line(dest, join_column_index, delim);

            //Hash key with simpleHash function. 
            int simpleHash_index = simpleHash(join_colum, MAX_BLOOM_FILTER);
            //Hash the key with djb2 function. 
            int djb2_index = djb2(join_colum, MAX_BLOOM_FILTER);
            //Hash the key with sdbm function. 
            int sdbm_index = sdbm(join_colum, MAX_BLOOM_FILTER);
            //Set the hashed-positions of the bloom filter to 1.
            bloomFilter[simpleHash_index] = 1;
            bloomFilter[djb2_index] = 1;
            bloomFilter[sdbm_index] = 1; 
        }*/


        /*
        for(int i=0; i < table_len-1; i++){
            char dest[1024];
            memset(dest, '\0', sizeof(dest));
            strncpy(dest, (t2 + line_start_index[i]), (line_start_index[i+1]-line_start_index[i]));
            
            char line[1024];
            memset(line, '\0', sizeof(line));
            strcpy(line, dest);
            
            char *join_colum = split_line(dest, join_column_index, delim);

            //Hash key with simpleHash function. 
            int simpleHash_index = simpleHash(join_colum, MAX_BLOOM_FILTER);
            //Hash the key with djb2 function. 
            int djb2_index = djb2(join_colum, MAX_BLOOM_FILTER);
            //Hash the key with sdbm function. 
            int sdbm_index = sdbm(join_colum, MAX_BLOOM_FILTER);
            //Set the hashed-positions of the bloom filter to 1.

            if(bloomFilter[simpleHash_index] == 1 
                && bloomFilter[djb2_index] == 1 
                && bloomFilter[sdbm_index] == 1){
                strcpy(candiate_tuples + cand_start_index[total_num_cand_tuples], line);
                
                total_num_cand_tuples++;
                cand_start_index[total_num_cand_tuples] = cand_start_index[total_num_cand_tuples -1] 
                                                         + strlen(line) + 1;
            }

        }*/