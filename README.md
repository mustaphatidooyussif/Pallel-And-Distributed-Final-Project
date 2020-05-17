# Parallel-Equi-Join
### Authors: Mustapha Tidoo Yussif and Dari Jones 
### Ashesi University, Ghana.
### Parallel and Distributed Final Project. 

## Problem Description
The simple idea is to develop and implement a paralle equi-join of two large relations R1(A,B) and R2(B,C) on their common join to form another 
relation C(A,B,C). The requirements of this project are:
1. The implementation must use a hybrid programming model. 
2. Must develop another implementation using different algorithm. 
3. A well written report discussing the implementation. 
4. A comparative performance analysis of the two solutions. 


## How to compile 
The mpicc compiler is used to compile this code. Make sure you have `mpitch` instaalled on you system first. After installing `mpitch`, run:
`mpicc ./main.c -o main -fopenmp -lgomp` 

## How to run. 
After compiling with the above command, run 
`mpiexec -n 3 main <path/to/relation/file> <path/to/relation2/file> <join attribute column index>` 

NB: The program can only run with three processes. More emphasis is place on threads to achieve the benefits of parallism. 