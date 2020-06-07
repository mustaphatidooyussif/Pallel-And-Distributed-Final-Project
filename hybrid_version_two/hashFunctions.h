#ifndef HASH_FUNCTIONS_H
#define HASH_FUNCTIONS_H
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

void add_item(char *key, char *line){

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

int num_items_in_bucket(char *key){
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

item ** find_bucket_items(char *key){

    int index = hash(key);

    item *ptr = hashTable[index];

    if (ptr == NULL){
        return NULL;
    }else{
        int i = 0;
        int numOfItems = num_items_in_bucket(key);
 
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
        printf("\n%d\n", num_items_in_bucket(ptr->key));
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
int simpleHash(char *key, int size){
    int hash = 0;
    
    for(unsigned int i=0; i < strlen(key); i++){
        hash += (int)key[i] * (i+1);
    }

    if (hash < 0){
        hash = hash * -1;
    }
    return (hash + 1)% size;
}

/*
The implementation of djb2 has function.
*/
int djb2(char *key, int size){
    
    int hash = 0;
    int c;

    while(( c=*key++)){
        hash = (int) ((hash << 5) + hash) + c;

    }

    if (hash < 0){
        hash = hash * -1;
    }

    return hash % size; 
}


/*
The implementation of sdbm hash function. 
*/
int sdbm(char *key, int size){
    
    int hash = 0;
    int c;

    while(( c=*key++)){
        hash = (int)((hash << 6) + (hash << 16)) - hash + c;
    }
    if (hash < 0){
        hash = hash * -1;
    }

    return hash % size; 
}


#endif 