#ifndef QUERIES_H 
#define QUERIES_H 

#include <stdlib.h> 
#include <stdio.h> 
#include <string.h> 
#include <omp.h> 

int numOfJoinTuples = 0;

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
          strcat(lineToJoin, found);
          strcat(lineToJoin, delim);
      }
        i ++; 
    }

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
    
    //#pragma omp parallel for schedule(static) 
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

#endif 