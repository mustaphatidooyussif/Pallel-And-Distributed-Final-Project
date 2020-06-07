#ifndef FILEMANAGER_H
#define FILEMANAGER_H

#include <stdlib.h> 
#include <stdio.h> 
#include <string.h> 

size_t TOTAL_NUM_LINES = 0;
int *LINES_START_INDEXES = NULL;

char *readFile(char *filename){

    //Try to OPen read only file
    FILE *f = fopen(filename, "rt");

    //If cannot open file
    if(f==NULL){
        fprintf(stderr, "Cannot open file\n");
        exit(EXIT_FAILURE);
    }

    //For reading each line from file. 

    //Read each line from file. 
    char *lineBuf = NULL;
    size_t n = 0;
    ssize_t lineLength = 0;
    size_t num_total_lines = 0; 
    size_t max_num_lines = 1000;
    size_t max_line_width = 1024;

    //Try to allocate space for table(table)
    char *table = (char*)malloc(max_num_lines * max_line_width *sizeof(char)); 

    //keep track of which line start where
    int *line_start_index = (int*)malloc(max_num_lines * sizeof(int));

    //If not successful 
    if(table == NULL){
        fprintf(stderr, "Unable to allocate memoery\n");
        exit(EXIT_FAILURE);
    }

    line_start_index[0] = 0;
    while((lineLength = getline(&lineBuf, &n, f)) != -1){

        //if exceeds allocated space
        if (num_total_lines >= max_num_lines){
            //increase number of rows and realloc 
            max_num_lines +=max_num_lines; 

            char *temp = realloc(table, max_num_lines * max_line_width*sizeof(char));

            int *temp2 = realloc(line_start_index, max_num_lines * sizeof(int));

            if (temp == NULL ){
                fprintf(stderr, "Unable to allocate memory\n");
                exit(EXIT_FAILURE);
            }
            
            if (temp2 == NULL ){
                fprintf(stderr, "Unable to allocate memory\n");
                exit(EXIT_FAILURE);
            }

            table = temp; 
            line_start_index = temp2;
        }

        //lineBuf[strcspn(lineBuf, "\n")] = '\0';
        
        lineBuf[strlen(lineBuf)-1] = '\0';

        //copy line into 
        strcpy(table + line_start_index[num_total_lines], lineBuf);
        num_total_lines ++; 
        line_start_index[num_total_lines] = line_start_index[num_total_lines-1]
                                             + lineLength + 1;
    }
    
    //table[num_total_lines] = NULL; //Mark end of table. 
    TOTAL_NUM_LINES = num_total_lines; //set the number of rows in table
    LINES_START_INDEXES = line_start_index;

    //free linebuf utilized by `getline()`
    free(lineBuf);
    //Close file. 
    fclose(f);
    return table; 
}

char *split_line(char *line, int index, const char *delim){

    int i= 0;
    char *joinColomn, *found;

    //Split line
    while( (found = strsep(&line, delim)) != NULL){

        //Stop if reached the join column. 
        if (i == index){
            joinColomn = found;
            break; 
        }
        i ++;
    }
    return joinColomn;
}

void deleteFile(char *filename){
    if(remove(filename) ==0 ){
        printf("Removing file %s...\n", filename);
    }else{
        printf("Error removing file.\n");
    }
}

#endif 