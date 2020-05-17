#include <stdlib.h> 
#include <stdio.h>
#include <string.h> 


int main(){
    char *key = "bac";

    int a = djb2(key);
    int b = sdbm(key);
    printf("%d, %d", a, b);

    return 0;
}


int djb2(char *key){
    int hash = 0;
    int c;

    while(( c=*key++)){
        hash = (int) ((hash << 5) + hash) + c;

    }

    return hash % 100; 
}

int sdbm(char *key){
    int hash = 0;
    int c;

    while(( c=*key++)){
        hash = (int) (c + (hash << 6) + (hash << 16) - hash);

    }

    return hash % 100;
}