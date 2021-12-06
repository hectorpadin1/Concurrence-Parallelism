#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <unistd.h>
#include <sys/types.h> 

int main(int argc, char *argv[])
{
    int n = 0;
    int procs, rank;
    MPI_Status status;
	pid_t id;

    MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &procs);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    id = getpid();

    printf("Process %d/%d: passed_num = %d (before).\n", rank, procs, n);
    
    MPI_Barrier(MPI_COMM_WORLD);

    if (id == 0) {
        n = 1;
    
        MPI_Send(&n,1,MPI_INT,1,0,MPI_COMM_WORLD);
        MPI_Recv(&n,1,MPI_INT,procs-1,0,MPI_COMM_WORLD,&status);
    
    } else {

        MPI_Recv(&n,1,MPI_INT,id-1,0,MPI_COMM_WORLD,&status);
        n++;
        MPI_Send(&n,1,MPI_INT,(id-1)%procs,0,MPI_COMM_WORLD);
    }

    printf("Process %d/%d: passed_num = %d (after).\n", id, procs, n);

    MPI_Finalize();
}