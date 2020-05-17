#include <stdlib.h> 
#include <stdio.h>
#include <string.h> 

/*Hbrid programming models.*/
#include "mpi.h"

#ifdef _OPENMP
#   include <omp.h> 
#endif 

/*User define header files.*/
#include "./fileManager.h"
#include "./bloomFilter.h"
#include "./queries.h"

#define NUM_THREADS 6

const char* delim = "|"; 
int recv_table_len = 0;

void sendTable(char **buf, int table_len, int dest, int tag);
char ** receiveTable(int source, int tag);
void receiveJoinCollection(int source, int tag, int joinColPos);

int main(int argc, char *argv[]){
    /*
    Main program logic goes here...
    */

    //Get arguments 
    if(argc != 4){
        fprintf(stderr, "Usage: mpiexec -n 3 program file1.txt file2.txt 2\n");
        exit(EXIT_FAILURE);
    }

    int comm_size; /*Number of processes*/
    int my_rank; /*Process id number*/ 
    char *r1_name = argv[1]; /*Name of file one.*/
    char *r2_name = argv[2]; /*Name of file two.*/
    int joinColPos = atoi(argv[3]) - 1; /*Position of join column.*/
    size_t r1_length = 0; /*Number of rows in table 1*/
    size_t r2_length = 0;  /*Number of rows in table 2*/
    unsigned int tupleSize = 0;
    char **r1 = NULL;
    char **r2 = NULL;

    unsigned int id = 0;
    unsigned int num_threads = 1; 
    
    //int n= omp_get_num_threads();
    //printf("%d\n", n);

    //Request number of threads
    omp_set_num_threads(NUM_THREADS);

    //Initialize bloom filter to zeros. 
    #pragma omp parallel for schedule(static) 
    for(unsigned int i=0; i < MAX_BLOOM_FILTER; i++){
        bloomFilter[i] = 0; 
    }

    double t1, t2; 

    /*Initialize the MPI environment*/
    MPI_Init(&argc, &argv);

    /**Get the number of processes*/
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size); 

    /*Get the rank of the process*/
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    int tag_zero = 0;
    int tag_one = 1;
    int tag_two = 2; 
    int tag_three = 3;
    int tag_four = 4;


    MPI_Barrier(MPI_COMM_WORLD);
    t1 = MPI_Wtime();
    /*
    STEP: 1
    Read the relations into two tables R1 and R2. And
    send R1 to node-1 and R2 to node-2. 
    */
    if(my_rank == 0){

        //Read file one into table
        r1 = readFile(r1_name, joinColPos);
        r1_length = num_rows;
        
        //Read file two into table
        r2 = readFile(r2_name, joinColPos);
        r2_length = num_rows;

        //TODO: send the small table to node one 

        if(r1_length <= r2_length){
            sendTable(r1, r1_length, 1, tag_zero);
            sendTable(r2, r2_length, 2, tag_zero);
        }else{
            sendTable(r2, r2_length, 1, tag_zero);
            sendTable(r1, r1_length, 2, tag_zero);
        }

        
        //free memory
        free(r1);
        free(r2);
    }else{
        //Process one try to receive 
        if (my_rank == 1){

            //STEP: TWO
            //Hash join column values and send bloom the filter to node 2.
            
            
            //Receive table from process 0
            char **r1_rec = receiveTable(0, tag_zero);

            //int id = omp_get_thread_num();
            //int nThreads = omp_get_num_threads();

            //int offset = id*recv_table_len/nThreads;

            //Prepare filter

            #pragma omp parallel
            {
                #pragma omp for schedule(static) 
                for(unsigned int i=0; i < recv_table_len; i++){
                    char line_1[1024];
                    strcpy(line_1, r1_rec[i]);
                    //printf("%s\n", r1_rec[i]);

                    char *joinColumn = splitLine(line_1, joinColPos, delim);
    
                    //Hash key with simpleHash function. 
                    //int simpleHash_index = simpleHash(joinColumn);
                    //Hash the key with djb2 function. 
                    //int djb2_index = djb2(joinColumn);
                    //Hash the key with sdbm function. 
                    //int sdbm_index = sdbm(joinColumn);

                    //Set the hashed-positions of the bloom filter to 1.
                    //bloomFilter[simpleHash_index] = 1;
                    //bloomFilter[djb2_index] = 1;
                    //bloomFilter[sdbm_index] = 1;  
                    #pragma omp critical 
                    addKey(joinColumn, bloomFilter);

                }

            }
            
            
            //Send the filled bloom filter to process 2
            MPI_Send(bloomFilter, MAX_BLOOM_FILTER, MPI_INT, 2, 
                    tag_zero, MPI_COMM_WORLD);

                    
            //STEP: FOUR
            //Receive possible join tuples from node 2 and perform 
            //equi-join. 
                printf("-------------------------------\n");

            receiveJoinCollection(2, tag_zero, joinColPos);

            //perform the equi-join and write the results to a file. 
            //DO equi-join 
            
            char **results = equiJoin(r1_rec, recv_table_len, joinColPos, delim);

            //Write results to file (R3.txt)
            char resultsFile[] = "R3.txt";

            //#pragma omp crical 
            writeIntoFile(results, numOfJoinTuples, resultsFile);
            
            //free the array of strings 
            
            #pragma omp parallel for schedule(static)
            for (size_t i=0; i< recv_table_len; i++){
                free(*(r1_rec + i));
            }

            free(r1_rec);

            //free array of strings 
            #pragma omp parallel for schedule(static)
            for(size_t j=0; j< numOfJoinTuples; j++){
                free(*(results + j));
            }
            free(results); 

        }else if(my_rank == 2){

            char **r2_rec = receiveTable(0, tag_zero);
            
            //STEP: THREE 
            //Find possible join tuples and send them to node 1.
            
            //Receive filled bloom filter from process 1. 
            int bloomFilter_rec[MAX_BLOOM_FILTER];
            MPI_Recv(bloomFilter_rec, MAX_BLOOM_FILTER, MPI_INT, 1,
                    tag_zero, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            //Collect tuples likely to be part of the join. 
            char **tuples = (char **)malloc(r2_length * sizeof(char *));
            if(tuples == NULL){
                fprintf(stderr, "Cannot allocate memory\n");
                exit(EXIT_FAILURE);
            }

            unsigned int index = 0;
            #pragma omp parallel for schedule(static)
            for(unsigned int i=0; i < recv_table_len; i ++){
                char line[1024];
                strcpy(line, r2_rec[i]);

                char *joinColumn = splitLine(line, joinColPos, delim);

                //Hash join column values using the three
                //hash functions and  check whether the 
                //corresponding positions in the bloom filter
                //are already set to 1s.  
                if(keyExist(joinColumn, bloomFilter_rec) ==1){
                    int lineSIze = strlen(r2_rec[i]);
                    
                    #pragma omp critical 
                    *(tuples + index) = malloc((lineSIze + 1) * sizeof(char));
                    
                    #pragma omp critical
                    strcpy(*(tuples + index), r2_rec[i]);

                    #pragma omp critical
                    index ++;
                }

            }

            printf("TBS=%d\n", index);
            sendTable(tuples, index, 1, tag_zero);

            //free array of strings 
            #pragma omp parallel for schedule(static)
            for(size_t i=0; i< recv_table_len; i++){
                free(*(r2_rec + i));
            }
            free(r2_rec);

            //free array of strings
            int numTuples = index;
            #pragma omp parallel for schedule(static)
            for(size_t  j=0; j < numTuples; j++){
                free(*(tuples +j));
            }

            free(tuples); 
        }else{
            fprintf(stderr, "Number of process mismatched\n");
            exit(EXIT_FAILURE);
        }
        
    }

    MPI_Barrier(MPI_COMM_WORLD);
    t2 = MPI_Wtime();

    //Finalize the MPI environment
    MPI_Finalize();

    f (my_rank ==0){
        printf("Parallel program ellapsed time: %f seconds\n", (t2-t1));

    }

    //free hashtable
    releaseHashTable();
    
    return 0;
}

void sendTable(char **buf, int table_len, int dest, int tag){
    MPI_Send(&table_len, 1, MPI_INT, dest, tag, MPI_COMM_WORLD);

    for(unsigned int i=0; i< table_len; i++){
        char line[1024];
        strcpy(line, buf[i]);

        char* line_ptr = buf[i];
        int line_len = strlen(line);

        MPI_Send(&line_len, 1, MPI_INT, dest, tag, MPI_COMM_WORLD);
        MPI_Send(line, strlen(line) + 1, MPI_CHAR, dest, tag, MPI_COMM_WORLD);
    }
}

char **receiveTable(int source, int tag){
    int table_len;

    MPI_Recv(&table_len, 1, MPI_INT, source, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    //Try to allocate space for relation(table)
    char **table = (char **)malloc(table_len * sizeof(char *)); 
    
    //If not successful 
    if(table == NULL){
        fprintf(stderr, "Unable to allocate memoery\n");
        exit(EXIT_FAILURE);
    }

    unsigned int j;
    for(j=0; j < table_len; j++){
        int line_len; 
        char line[1024];
        memset(line, 0, sizeof(line));
        MPI_Recv(&line_len, 1, MPI_INT, source, tag, 
                MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(line, line_len + 1, MPI_CHAR, tag, 
                source, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        //Add lines to table
        *(table + j) = (char *)malloc((line_len+1) * sizeof(char));
        
        strcpy(*(table + j), line);         
    }

    //set number of rows received. 
    recv_table_len = j; 
    return table;
}

void receiveJoinCollection(int source, int tag, int joinColPos){
    //printf("Entered\n");
    int collection_len;
    MPI_Recv(&collection_len, 1, MPI_INT, source, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    //printf("============================%d\n", collection_len);

    for(unsigned int k=0; k < collection_len; k++){
        int line_len; 
        char line[1024];
        //printf("i=%d, size=%d\n",k, line_len);

        MPI_Recv(&line_len, 1, MPI_INT, source, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(line, line_len + 1, MPI_CHAR, source, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        //Add items to hash table 
        char cp_line[1024];

        strcpy(cp_line, line);

        char *joinColumn = splitLine(cp_line, joinColPos, delim);

        addItem(joinColumn, line); 

    }
}
