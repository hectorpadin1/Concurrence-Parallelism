#include <mpi.h>
#include <stdio.h>

int main(int argc, char *argv[]) {
	int mpi_size, mpi_rank;

	MPI_Init(&argc, &argv);

	MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
	MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

	printf("Hello world from process %d out %d\n", mpi_rank, mpi_size);

	MPI_Finalize();

	return 0;
}