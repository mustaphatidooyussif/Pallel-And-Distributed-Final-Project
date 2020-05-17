#include <stdlib.h> 
#include <stdio.h>
#include <string.h> 
#include <time.h> 
#include <sys/time.h> 

/*User define header files.*/
#include "./fileManager.h"

#define BLOOMFILTER_H
#define MAX_BLOOM_FILTER 100000
#define HASH_TABLE_SIZE 100000

int numOfJoinTuples = 0;
const char* delim = "|"; 

//Hash table size 

typedef struct _item{
    char *key;
    char *line;
    struct _item *next;
} item; 


char *lineFromR2WithoutJoinColumn(char *line, int index, const char *delim);
char ** equiJoin(char **table_1, int table_1_ln, int joinColPos, const char* delim);

int keyExist(char *key, int bloom[]);
void addKey(char *key, int bloom[]);
int sdbm(char *key);
int djb2(char *key);
int simpleHash(char *key);
void releaseHashTable();
void printHashTable();
void printItemsInBucket(int index);
int numOfItemsInBucket(char *key);
void addItem(char *key, char *line);
int hash(char *key);
item ** findItemsInBucket(char *key);


item* hashTable[HASH_TABLE_SIZE];


//Create and initialize bloom filter to zeroes. 
int bloomFilter[MAX_BLOOM_FILTER]; 

int main(int argc, char *argv[]){
    /*
    Main program logic goes here...
    */

    //Get arguments 
    if(argc != 4){
        fprintf(stderr, "Usage: program file1.txt file2.txt 2\n");
        exit(EXIT_FAILURE);
    }


    char *r1_name = argv[1]; /*Name of file one.*/
    char *r2_name = argv[2]; /*Name of file two.*/
    int joinColPos = atoi(argv[3]) - 1; /*Position of join column.*/
    size_t r1_length = 0; /*Number of rows in table 1*/
    size_t r2_length = 0;  /*Number of rows in table 2*/
    unsigned int tupleSize = 0;
    char **r1 = NULL;
    char **r2 = NULL;
    int numTuples = 0;
    time_t w_time; 

    for(unsigned int i=0; i < MAX_BLOOM_FILTER; i++){
        bloomFilter[i] = 0; 
    }

    struct timeval tv1, tv2; 
    gettimeofday(&tv1, NULL);

    w_time = clock();

    //Read file one into table
    r1 = readFile(r1_name, joinColPos);
    r1_length = num_rows;
    printf("Table 1 size = %d\n", r1_length);

    for(unsigned int i=0; i < r1_length; i++){
        char line_1[1024];
        strcpy(line_1, r1[i]);
        char *joinColumn = splitLine(line_1, joinColPos, delim);

        addKey(joinColumn, bloomFilter);
    }

    //Read file two into table
    r2 = readFile(r2_name, joinColPos);
    r2_length = num_rows;
    printf("Table 2 size = %d\n", r2_length);
            
    //Collect tuples likely to be part of the join. 
    char **tuples = (char **)malloc(r2_length * sizeof(char *));
    if(tuples == NULL){
        fprintf(stderr, "Cannot allocate memory\n");
        exit(EXIT_FAILURE);
    }
    
    unsigned int index = 0;
    for(unsigned int i=0; i < r2_length; i ++){
        char line[1024];
        strcpy(line, r2[i]);

        char *joinColumn = splitLine(line, joinColPos, delim);

        //Hash join column values using the three
        //hash functions and  check whether the 
        //corresponding positions in the bloom filter
        //are already set to 1s.  
        if(keyExist(joinColumn, bloomFilter) ==1){
            int lineSIze = strlen(r2[i]);
            *(tuples + index) = malloc((lineSIze + 1) * sizeof(char));
            strcpy(*(tuples + index), r2[i]);
            index ++;
        }

    }
    numTuples = index;
    
    //Put in hashtable for easy access
    for(unsigned int k=0; k < numTuples; k++){
        //Add items to hash table 
        char cp_line[1024];
        strcpy(cp_line, tuples[k]);

        char *joinColumn = splitLine(cp_line, joinColPos, delim);
        addItem(joinColumn, tuples[k]); 

    }
    
    char **results = equiJoin(r1, r1_length, joinColPos, delim);

    
    //Write results to file (R3.txt)
    char resultsFile[] = "Serial_R3.txt";

    writeIntoFile(results, numOfJoinTuples, resultsFile);   

           
    //free the array of strings 
    for (size_t i=0; i< r1_length; i++){
        free(*(r1 + i));
    }

    free(r1);

    //free array of strings 
    for(size_t j=0; j< numOfJoinTuples; j++){
        free(*(results + j));
    }
    free(results);

    //free array of strings 
    for(size_t i=0; i< r2_length; i++){
        free(*(r2 + i));
    }
    free(r2);

    //free array of strings
    for(size_t  j=0; j < numTuples; j++){
        free(*(tuples +j));
    }

    free(tuples);

    //free hashtable
    releaseHashTable();
    gettimeofday(&tv2, NULL);

    w_time = clock() - w_time; 


    printf("Serial Program elapsed time: %f\n",((double)w_time)/CLOCKS_PER_SEC);
    
    printf("THE TIME=%f \n", 
            (double)(tv2.tv_usec - tv1.tv_usec)/CLOCKS_PER_SEC 
            + (double)(tv2.tv_sec - tv1.tv_sec)); 
    return 0;

}


char *lineFromR2WithoutJoinColumn(char *line, int index, const char *delim)
{

    int i= 0;
    char *found;
    //Try to allocate space for relation(table)
    char *lineToJoin = (char *)malloc(1024 * sizeof(char)); 

    //If not successful 
    if(lineToJoin == NULL){
        fprintf(stderr, "Unable to allocate memoery\n");
        exit(EXIT_FAILURE);
    }

    strcpy(lineToJoin, "");

    //Split line
    while( (found = strsep(&line, delim)) != NULL){
      if(index != i){
          strcat(lineToJoin, delim);
          strcat(lineToJoin, found);
      }
        i ++; 
    }
    //strcat(lineToJoin, "\n");
    return lineToJoin;
}


char ** equiJoin(char **table_1, int table_1_ln, int joinColPos, const char* delim){
    
    //Try to allocate space for relation(table)
    size_t index = 0;
    size_t resultsSize = 100;    
    char **results = (char **)malloc(resultsSize * sizeof(char *)); 
    
    //If not successful 
    if(results == NULL){
        fprintf(stderr, "Unable to allocate memoery\n");
        exit(EXIT_FAILURE);
    }
    
    for(unsigned int i=0; i < table_1_ln; i++){

        char line[1024];
        strcpy(line, table_1[i]);
        
        char *joinColumn = splitLine(line, joinColPos, delim);
        
        int numOfItems = numOfItemsInBucket(joinColumn);

        
        //if there items
        if (numOfItems != 0){
            item **joinLines = findItemsInBucket(joinColumn);
            
            
            for(unsigned int j=0; j< numOfItems; j++){
                
                
                //Get start and end indexes of join column in line
                char line_copy[1024];
                strcpy(line_copy, joinLines[j]->line);

                char *lineToJoin = lineFromR2WithoutJoinColumn(
                               line_copy, joinColPos, delim);
                
                //Reallocate memory
                if (index >= resultsSize){
                    //increase number of rows and realloc 
                    resultsSize +=resultsSize; 

                    char **temp = realloc(results, resultsSize*sizeof(char *));
                    if (temp == NULL ){
                        fprintf(stderr, "Unable to allocate memory\n");
                        exit(EXIT_FAILURE);
                    }

                    results = temp; 
                }
                
                //join lines from R1 and R2
                char result[1024];
                table_1[i][strlen(table_1[i])-1] = '\0';
                strcpy(result, table_1[i]);

                lineToJoin[strlen(lineToJoin)-1] = '\0';
                strcat(result, lineToJoin);
                strcat(result, "\n");

                free(lineToJoin);
                
                *(results + index)  = malloc(1024 * sizeof(char));

                //copy line into 
                strcpy(*(results + index), result);

                index ++;
                

            }
        } 
    } 
    numOfJoinTuples = index; 


    return results;
}


int hash(char *key){
    int hashVal = 0;
    
    for(unsigned int i=0; i < strlen(key); i++){
        hashVal += (int)key[i] * (i+1);
    }

    return hashVal % HASH_TABLE_SIZE;  
}

void addItem(char *key, char *line){

    int index = hash(key);
    
    item *n = malloc(sizeof(item));

    //add key
    n->key = malloc(100*sizeof(char));
    strcpy(n->key, key);

    //add line
    n->line = malloc(1024*sizeof(char)); 
    strcpy(n->line, line);

    //net item
    n->next = NULL;
    
    //Insert if index has no item.
    if (hashTable[index] == NULL){
        hashTable[index] = n; 
    }else{
        
        //Loop to the appropriate place to insert.
        item *ptr = hashTable[index];
        
        
        while(ptr->next != NULL){
            ptr = ptr->next;
        }
        
        ptr->next = n;
        
    } 
}


int numOfItemsInBucket(char *key){
    int index = hash(key);
    int count = 0;

    //no item in current index
    if (hashTable[index] == NULL){
        return count;
    }else{
        count ++;

        //count all items in this bucket.
        item *ptr = hashTable[index];

        while(ptr->next != NULL){
            count ++;
            ptr = ptr->next;
        }
        return count;
    }
}

item ** findItemsInBucket(char *key){

    int index = hash(key);

    item *ptr = hashTable[index];

    if (ptr == NULL){
        return NULL;
    }else{
        int i = 0;
        int numOfItems = numOfItemsInBucket(key);
 
        item **items = (item **)malloc(numOfItems * sizeof(item*)); 

        //If not successful 
        if(items == NULL){
            fprintf(stderr, "Unable to allocate memoery\n");
            exit(EXIT_FAILURE);
        }

        items[i] = ptr; 

        while(ptr->next !=NULL){
            ptr =  ptr->next;
            i ++;
            
            //allocate space and add item
            items[i] = malloc(1024 * sizeof(item));
            items[i] =  ptr;
        }

        return items; 
    }
    

}

void printItemsInBucket(int index){

    item *ptr = hashTable[index];

    while(ptr != NULL){
        printf("\n-------------------------------------------------\n");
        printf("\n%d\n", numOfItemsInBucket(ptr->key));
        printf("\n%s\n", ptr->key);
        printf("\n%s\n", ptr->line);
        printf("\n-------------------------------------------------\n");
        ptr = ptr->next; 
    }
}


void printHashTable(){
    for(unsigned int i=0; i < HASH_TABLE_SIZE; i++){
        printItemsInBucket(i);
    }
}


//Release allocated memory for hash table
void releaseHashTable(){
    for(unsigned int i=0; i < HASH_TABLE_SIZE; i++){
        item *ptr = hashTable[i];
        item *temp = NULL; 

        //free memory in bucket. 
        while(ptr != NULL){
            temp = ptr->next; 

            //free item
            free(ptr->key);
            free(ptr->line);
            free(ptr);

            ptr = temp; 
        }
    }
}

/*
The implementation of simpleHash hash function
*/

int simpleHash(char *key){
    int hash = 0;
    
    for(unsigned int i=0; i < strlen(key); i++){
        hash += (int)key[i] * (i+1);
    }

    if (hash < 0){
        hash = hash * -1;
    }
    return (hash + 1)% MAX_BLOOM_FILTER;
}

/*
The implementation of djb2 has function.
*/
int djb2(char *key){
    
    int hash = 0;
    int c;

    while(( c=*key++)){
        hash = (int) ((hash << 5) + hash) + c;

    }

    if (hash < 0){
        hash = hash * -1;
    }

    return hash % MAX_BLOOM_FILTER; 

    /*
    int index = 0;
    
    for(unsigned int i=0; i < strlen(key); i++){
        index += (int)key[i] * (i+1);
    }


    return (index + 2)% MAX_BLOOM_FILTER; */
}


/*
The implementation of sdbm hash function. 
*/


int sdbm(char *key){
    
    int hash = 0;
    int c;

    while(( c=*key++)){
        hash = (int)((hash << 6) + (hash << 16)) - hash + c;
    }
    if (hash < 0){
        hash = hash * -1;
    }

    return hash % MAX_BLOOM_FILTER; 
    
    /*
    int index = 0;
    
    for(unsigned int i=0; i < strlen(key); i++){
        index += (int)key[i] * (i+1);
    }


    return (index + 3)% MAX_BLOOM_FILTER;*/
}

void addKey(char *key, int bloom[]){

    //Hash key with simpleHash function. 
    int simpleHash_index = simpleHash(key);
    //Hash the key with djb2 function. 
    int djb2_index = djb2(key);
    //Hash the key with sdbm function. 
    int sdbm_index = sdbm(key);

    //Set the hashed-positions of the bloom filter to 1.
    bloom[simpleHash_index] = 1;
    bloom[djb2_index] = 1;
    bloom[sdbm_index] = 1;  

}

int keyExist(char *key, int bloom[]){

    int exist = 0;

    //Hash key with simpleHash function. 
    int simpleHash_index = simpleHash(key);

    //Hash the key with djb2 function. 
    int djb2_index = djb2(key);

    //Hash the key with sdbm function. 
    int sdbm_index = sdbm(key);

    /*If values at hashed-positions of bloom filters are 1s. 
    Set exist =1, and return 
    */
    if(bloom[simpleHash_index] == 1 
        && bloom[djb2_index] == 1 
        && bloom[sdbm_index] == 1){
            exist = 1;
        }


    return exist; 
}
