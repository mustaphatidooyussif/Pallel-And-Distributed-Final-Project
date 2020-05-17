#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H
#define MAX_BLOOM_FILTER 100000
#define HASH_TABLE_SIZE 100000

#include <stdlib.h> 
#include <stdio.h> 
#include <string.h> 

//Hash table size 

typedef struct _item{
    char *key;
    char *line;
    struct _item *next;
} item; 

item* hashTable[HASH_TABLE_SIZE];

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

//Create and initialize bloom filter to zeroes. 
int bloomFilter[MAX_BLOOM_FILTER]; 

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


#endif 