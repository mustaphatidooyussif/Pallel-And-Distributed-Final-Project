
#include <stdlib.h> 
#include <stdio.h>
#include <string.h> 

/*Hbrid programming models.*/
#include "mpi.h"

#include <omp.h> 

#define MAX_BLOOM_FILTER 100000
#define NUM_THREADS 2

/*User define header files.*/
#include "./fileManager.h"
#include "./hashFunctions.h"

const char* delim = "|";

char *r2_line(char *, int , const char *);
void send_table(char *, int , int , int ,int , int *);

int main(int argc, char *argv[]){

    //Get arguments 
    if(argc != 4){
        fprintf(stderr, "Usage: mpiexec -n 3 program file1.txt file2.txt 2\n");
        exit(EXIT_FAILURE);
    }

    int comm_size; /*Number of processes*/
    int my_rank; /*Process id number*/ 
    char *file_1 = argv[1]; /*Name of file one.*/
    char *file_2 = argv[2]; /*Name of file two.*/
    int join_column_index = atoi(argv[3]) - 1; /*Position of join column.*/

    double t1, t2; 
    
    /*Initialize the MPI environment*/
    MPI_Init(&argc, &argv);

    /**Get the number of processes*/
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size); 

    /*Get the rank of the process*/
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    MPI_Barrier(MPI_COMM_WORLD);
    t1 = MPI_Wtime();

    if(my_rank == 0){
        printf("Process no. %d\n", my_rank);
        int max_line_width = 1024;

        //Read and send table to process 1
        char *t1 = readFile(file_1);
        int t1_len = TOTAL_NUM_LINES;
        int *line_start_indexes_file1 = LINES_START_INDEXES;

        send_table(t1, t1_len,1, 1, max_line_width, line_start_indexes_file1);

        //Read and send table to process2
        char *t2 = readFile(file_2);
        int t2_len = TOTAL_NUM_LINES;
        int *line_start_indexes_file2 = LINES_START_INDEXES;

        if (t1_len <= t2_len){
            send_table(t1, t1_len,1, 1, max_line_width, line_start_indexes_file1);
            send_table(t2, t2_len, 2, 1, max_line_width, line_start_indexes_file2);
        }else{
            send_table(t1, t1_len,2, 1, max_line_width, line_start_indexes_file1);
            send_table(t2, t2_len, 1, 1, max_line_width, line_start_indexes_file2);
        }

        free(t1);
        free(t2);
        
    }else if(my_rank == 1){
        printf("Process no. %d\n", my_rank);
        int max_line_width = 1024;
        int table_len;
        MPI_Status status;
        MPI_Recv(&table_len, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, &status);
        char *t1 = (char*)malloc(table_len * max_line_width *sizeof(char)); 
        int *line_start_index = (int*)malloc(table_len * sizeof(int));

        MPI_Recv(t1, table_len * max_line_width, MPI_CHAR, 0, 2, MPI_COMM_WORLD, &status);
        MPI_Recv(line_start_index, table_len, MPI_INT, 0, 3, MPI_COMM_WORLD, &status);

        
        //Create and initialize bloom filter to zeroes. 
        int *bloomFilter = (int*)calloc(MAX_BLOOM_FILTER, sizeof(int));

        //Cannot paralize this loop with omp because there is 
        //loop carry dependendance inside `strncpy`.

        omp_set_dynamic(0);
        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            int n_threads =  omp_get_num_threads();

            int portion = table_len/n_threads; 
            int my_start = tid * portion; 
            int my_end = (tid + 1) * portion;

            //if last thread, 
            if(tid == n_threads -1){my_end = table_len -1;}

            for(int i=my_start; i < my_end; i++){
                char dest[1024];
                memset(dest, '\0', sizeof(dest));
                strncpy(dest, (t1 + line_start_index[i]), 
                            (line_start_index[i+1]-line_start_index[i]));

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

            }
        }

        //send filled bloom filter
        MPI_Send(bloomFilter, MAX_BLOOM_FILTER, MPI_INT, 2, 1, MPI_COMM_WORLD);
        
        //receive candidate tuples 
        int num_cand_tuples;
        MPI_Recv(&num_cand_tuples, 1, MPI_INT, 2, 1, MPI_COMM_WORLD, &status);
        char *cand_tuples = (char*)malloc(num_cand_tuples * max_line_width *sizeof(char));
        int *cand_start_index = (int*)malloc(num_cand_tuples * sizeof(int));

        MPI_Recv(cand_tuples, num_cand_tuples * max_line_width, MPI_CHAR, 2, 2, MPI_COMM_WORLD, &status);
        MPI_Recv(cand_start_index, num_cand_tuples, MPI_INT, 2, 3, MPI_COMM_WORLD, &status);
        
        
        //Put candidate tuples in hashtable
        omp_set_dynamic(0);
        #pragma omp parallel
        {   
            int tid = omp_get_thread_num();
            int n_threads =  omp_get_num_threads();

            int portion = num_cand_tuples/ n_threads; 
            int my_start = tid * portion; 
            int my_end = (tid + 1) * portion;

            //if last thread, 
            if(tid == n_threads -1){my_end = num_cand_tuples -1;}

            for(int j=my_start; j< my_end; j++){
                char line[1024]; 
                memset(line , '\0', sizeof(line)); 
                strncpy(line, (cand_tuples + cand_start_index[j]), 
                                (cand_start_index[j+1]-cand_start_index[j]));

                char line_2[1024];
                memset(line_2, '\0', sizeof(line_2));
                strcpy(line_2, line);
            
                char *join_colum = split_line(line, join_column_index, delim);

                #pragma omp critical 
                add_item(join_colum, line_2);
            }
        }

        char *filename ="./R3.txt";
        deleteFile(filename);

        FILE *f = fopen(filename, "w");

        //If cannot open file
        if(f==NULL){
            fprintf(stderr, "Cannot open file\n");
            exit(EXIT_FAILURE);
        }

        //do equijoin in parallel 
        omp_set_dynamic(0);
        #pragma omp parallel 
        {
            int tid = omp_get_thread_num();
            int n_threads =  omp_get_num_threads();

            int portion = table_len/ n_threads; 

            int my_start = tid * portion; 
            int my_end = (tid + 1) * portion;
            //if last thread, 
            if(tid == n_threads -1){my_end = table_len -1;}

            for(int j=my_start; j< my_end; j++){
                char temp[1024];
                memset(temp, '\0', sizeof(temp));
                strncpy(temp, (t1 + line_start_index[j]), 
                            (line_start_index[j+1]-line_start_index[j]));

                char r1_tuple[1024];
                memset(r1_tuple, '\0', sizeof(r1_tuple));
                strcpy(r1_tuple, temp);

                char *join_colum = split_line(temp, join_column_index, delim);
                int num_items = num_items_in_bucket(join_colum);   
                
                if(num_items != 0){
                    
                    item **join_lines = find_bucket_items(join_colum);
                    
                    for(int k=0; k < num_items; k++){
                        
                        char line_copy[1024];
                        
                        strcpy(line_copy, join_lines[k]->line); 
                        
                        //get the line from r2
                        char *cand_r2_line = r2_line(
                                join_lines[k]->line, join_column_index, delim);
                            
                        //join lines from R1 and R2
                        char result[1024];
                        r1_tuple[strlen(r1_tuple)-1] = '\0';
                        strcpy(result, r1_tuple);
                        
                        cand_r2_line[strlen(cand_r2_line)-1] = '\0';
                        strcat(result, cand_r2_line);
                        strcat(result, "\n");
                        
                        #pragma omp critical 
                        fputs(result, f);
                        
                      }
                    } 
                }
        }
        
        //close file
        fclose(f);

        free(t1);
        free(line_start_index);
        //free(cand_tuples);

    }else if(my_rank == 2){
        printf("Process no. %d\n", my_rank);
        int max_line_width = 1024;
        int table_len;
        MPI_Status status;
        MPI_Recv(&table_len, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, &status);
        char *t2 = (char*)malloc(table_len * max_line_width *sizeof(char)); 
        int *line_start_index = (int*)malloc(table_len * sizeof(int));

        MPI_Recv(t2, table_len * max_line_width, MPI_CHAR, 0, 2, MPI_COMM_WORLD, &status);
        MPI_Recv(line_start_index, table_len, MPI_INT, 0, 3, MPI_COMM_WORLD, &status);
        
        //Receive filled bloom filter
        int *bloomFilter = (int*)malloc(MAX_BLOOM_FILTER * sizeof(int));
        MPI_Recv(bloomFilter, MAX_BLOOM_FILTER, MPI_INT, 1, 1, MPI_COMM_WORLD, &status);

        //Collect candidate tuples.
        char *candiate_tuples = (char*)malloc(table_len * max_line_width *sizeof(char)); 

        //keep track of which line start where
        int *cand_start_index = (int*)malloc(table_len * sizeof(int));

        cand_start_index[0] = 0;
        int total_num_cand_tuples = 0;

        omp_set_dynamic(0);
        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            int n_threads =  omp_get_num_threads();

            int portion = table_len/n_threads; 
            int my_start = tid * portion; 
            int my_end = (tid + 1) * portion;

            //if last thread, 
            if(tid == n_threads -1){my_end = table_len -1;}

            for(int i=my_start; i < my_end; i++){
                char dest[1024];
                memset(dest, '\0', sizeof(dest));
                strncpy(dest, (t2 + line_start_index[i]), 
                            (line_start_index[i+1]-line_start_index[i]));

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

            }
        }
        
        //send candidate tuples
        MPI_Send(&total_num_cand_tuples, 1, MPI_INT, 1, 1, MPI_COMM_WORLD);
        MPI_Send(candiate_tuples, total_num_cand_tuples * max_line_width, MPI_CHAR, 1, 2, MPI_COMM_WORLD);
        MPI_Send(cand_start_index, total_num_cand_tuples, MPI_INT, 1, 3, MPI_COMM_WORLD);
        
        
        free(t2);
        free(line_start_index);
        //free(candiate_tuples);
    }else{
        printf("Invalid process number\n");
    }

    MPI_Barrier(MPI_COMM_WORLD);
    t2 = MPI_Wtime();

    //Finalize the MPI environment
    MPI_Finalize();

    if (my_rank ==0){
        printf("Parallel program ellapsed time: %f seconds\n", (t2-t1));

    }

 return 0;
}


char *r2_line(char *line, int index, const char *delim)
{

    int i= 0;
    char *found;
    //Try to allocate space for relation(table)
    char *cand_r2_line = (char *)malloc(1024 * sizeof(char)); 

    //If not successful 
    if(cand_r2_line == NULL){
        fprintf(stderr, "Unable to allocate memoery\n");
        exit(EXIT_FAILURE);
    }

    strcpy(cand_r2_line, "");

    //Split line
    while( (found = strsep(&line, delim)) != NULL){
      if(index != i){
          strcat(cand_r2_line, found);
          strcat(cand_r2_line, delim);
      }
        i ++; 
    }

    return cand_r2_line;
}

void send_table(char *table, int table_len, int dest, int tag,
                 int line_width, int *line_start_indexes){
    MPI_Send(&table_len, 1, MPI_INT, dest, tag, MPI_COMM_WORLD);
    MPI_Send(table, table_len * line_width, MPI_CHAR, dest, tag + 1, MPI_COMM_WORLD);
    MPI_Send(line_start_indexes, table_len, MPI_INT, dest, tag + 2, MPI_COMM_WORLD);
         
}