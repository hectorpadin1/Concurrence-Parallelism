#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include "compress.h"
#include "chunk_archive.h"
#include "queue.h"
#include "options.h"

#define CHUNK_SIZE (1024*1024)
#define QUEUE_SIZE 20

#define COMPRESS 1
#define DECOMPRESS 0


struct th_info {
    pthread_t       th_id;                  // id returned by pthread_create()
    int             th_num;                 // application defined thread #
};

struct readCmp {
	int 			chunks;
	int 			size;
	int 			fd;
	queue 			*in;
};

struct readDcmp {
	archive 		ar;
	queue 			*in;
}; 

struct writeCmp {
	archive 		ar;
	queue 			*out;
	int 			chunks;
};

struct writeDcmp {
	archive 		ar;
	queue 			*out;
	int 			fd;
};

struct args {
    int             th_num;
    queue           *in;
    queue           *out;
    int             *chunks;
    pthread_mutex_t *mutex;
    chunk (*process)(chunk);
};


// read input file and send chunks to the in queue
void *rdComp(void *ptr) {
	struct readCmp *argR = (struct readCmp*) ptr;
	chunk ch;
	int off, i; 

    for(i=0; i<argR->chunks; i++) {
        ch = alloc_chunk(argR->size);

        off=lseek(argR->fd, 0, SEEK_CUR);

        ch->size   = read(argR->fd, ch->data, argR->size);
        ch->num    = i;
        ch->offset = off;

        q_insert(*argR->in, ch);
    }
    return NULL;
}

// send chunks to the output archive file
void *wrComp(void *ptr) {
	struct writeCmp *argW = (struct writeCmp*) ptr;
	chunk ch;
	int i;

    for(i=0; i<argW->chunks; i++) {
        ch = q_remove(*argW->out);

        add_chunk(argW->ar, ch);
        free_chunk(ch);
    }
    return NULL;
}

// read chunks with compressed data
void *rdUncmp(void *ptr) {
	struct readDcmp *rdD = (struct readDcmp*) ptr;
	chunk ch;
	int i;

	for (i=0; i<chunks(rdD->ar); ++i) {
		ch = get_chunk(rdD->ar, i);
		q_insert(*rdD->in, ch);
	}
	return NULL;
}

// write chunks from output to decompressed file
void *wrUncmp(void *ptr) {
	struct writeDcmp *wrD = (struct writeDcmp*) ptr;
	chunk ch;
	int i;

	for (i=0; i<chunks(wrD->ar); ++i) {
		ch = q_remove(*wrD->out);
		lseek(wrD->fd, ch->offset, SEEK_SET);
		write(wrD->fd, ch->data, ch->size);
		free_chunk(ch);
	}
	return NULL;
}

// take chunks from queue in, run them through process (compress or decompress)
// send them to queue out
void *worker(void *ptr) {
    struct args *args = (struct args*) ptr;
    chunk ch, res;

    while(1) {
        pthread_mutex_lock(args->mutex);
        if (*args->chunks) {
            (*args->chunks)--;
            pthread_mutex_unlock(args->mutex);
            
            ch = q_remove(*args->in);

            res = args->process(ch);
            free_chunk(ch);

            q_insert(*args->out, res);
	    } else {
	        pthread_mutex_unlock(args->mutex);
	        break;
	    }
    }
    return NULL;
}

// Compress file taking chunks of opt.size from the input file,
// inserting them into the in queue, running them using a worker,
// and sending the output from the out queue into the archive file
void comp(struct options opt) {
    int fd, chunks, i, offset, chnk;
    struct stat st;
    struct th_info *threads, rd_th, wr_th;
    struct args *args;
    struct readCmp rdC;
    struct writeCmp wrC;
    char comp_file[256];
    archive ar;
    queue in, out;
    chunk ch;
    pthread_mutex_t mut;

    if((fd=open(opt.file, O_RDONLY))==-1) {
        printf("Cannot open %s\n", opt.file);
        exit(0);
    }

    fstat(fd, &st);
    chunks = st.st_size/opt.size+(st.st_size % opt.size ? 1:0);

    chnk = chunks;

    if(opt.out_file) {
        strncpy(comp_file,opt.out_file,255);
    } else {
        strncpy(comp_file, opt.file, 255);
        strncat(comp_file, ".ch", 255);
    }

    ar = create_archive_file(comp_file);

    in  = q_create(opt.queue_size);
    out = q_create(opt.queue_size);

    // allocate memory for threads and args
    threads = malloc(opt.num_threads*sizeof(struct th_info));
    args = malloc(opt.num_threads*sizeof(struct args));

    if (threads == NULL || args==NULL) {
        printf("Not enough memory\n");
        exit(1);
    }


    pthread_mutex_init(&mut,NULL);

    // creates a thread to run rdComp
	rdC.chunks 	= chunks;
	rdC.in 		= &in;
	rdC.size 	= opt.size;
	rdC.fd 		= fd;
	pthread_create(&rd_th.th_id, NULL, rdComp, &rdC);

    // create threads running worker()
    for (i = 0; i < opt.num_threads; i++) {
        
        threads[i].th_num = i;
        args[i].th_num  = i;
        args[i].in      = &in;
        args[i].out     = &out;
        args[i].chunks  = &chnk;
        args[i].mutex   = &mut;
        args[i].process = zcompress;

        if (0 != pthread_create(&threads[i].th_id, NULL,
                     worker, &args[i])) {
            printf("Could not create thread #%d", i);
            exit(1);
        }
    }

	// creates a thread to run wrComp
	wrC.chunks 	= chunks;
	wrC.out 	= &out;
	wrC.ar 		= ar;
	pthread_create(&wr_th.th_id, NULL, wrComp, &wrC);

    pthread_join(rd_th.th_id, NULL);

    // Wait for the threads to finish
    for (i = 0; i<opt.num_threads; i++)
        pthread_join(threads[i].th_id, NULL);
    
    pthread_join(wr_th.th_id, NULL);


    close_archive_file(ar);
    close(fd);
    q_destroy(in);
    q_destroy(out);
    pthread_mutex_destroy(&mut);
    free(threads);
    free(args);

    pthread_exit(NULL);
}


// Decompress file taking chunks of opt.size from the input file,
// inserting them into the in queue, running them using a worker,
// and sending the output from the out queue into the decompressed file
void decomp(struct options opt) {
    int fd, chnk, i;
    struct stat st;
    struct th_info *threads, th_rd, th_wr;
    struct args *args;
    struct readDcmp rdD;
    struct writeDcmp wrD;
    char uncomp_file[256];
    archive ar;
    queue in, out;
    chunk ch;
    pthread_mutex_t mut;

    if((ar=open_archive_file(opt.file))==NULL) {
        printf("Cannot open archive file\n");
        exit(0);
    }

    if(opt.out_file) {
        strncpy(uncomp_file, opt.out_file, 255);
    } else {
        strncpy(uncomp_file, opt.file, strlen(opt.file) -3);
        uncomp_file[strlen(opt.file)-3] = '\0';
    }

    if((fd=open(uncomp_file, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH))== -1) {
        printf("Cannot create %s: %s\n", uncomp_file, strerror(errno));
        exit(0);
    }

    in  = q_create(opt.queue_size);
    out = q_create(opt.queue_size);

    threads = malloc(opt.num_threads*sizeof(struct th_info));
    args = malloc(opt.num_threads*sizeof(struct args));

    if (threads == NULL || args==NULL) {
        printf("Not enough memory\n");
        exit(1);
    }

    pthread_mutex_init(&mut,NULL);

    chnk = chunks(ar);


    rdD.ar 		= ar;
    rdD.in 		= &in;
    wrD.ar 		= ar;
    wrD.out 	= &out;
    wrD.fd		= fd;
    // creates a thread to run rdUncmp
    pthread_create(&th_rd.th_id, NULL, rdUncmp, &rdD);

    // create threads running worker()
    for (i = 0; i < opt.num_threads; i++) {
        threads[i].th_num = i;

        args[i].th_num  = i;
        args[i].in      = &in;
        args[i].out     = &out;
        args[i].chunks  = &chnk;
        args[i].mutex   = &mut;
        args[i].process = zdecompress;

        if (0 != pthread_create(&threads[i].th_id, NULL,
                     worker, &args[i])) {
            printf("Could not create thread #%d", i);
            exit(1);
        }
    }

    // creates a thread to run wrUncmp
    pthread_create(&th_wr.th_id, NULL, wrUncmp, &wrD);

    pthread_join(th_rd.th_id, NULL);

    // Wait for the threads to finish
    for (i = 0; i<opt.num_threads; i++)
        pthread_join(threads[i].th_id, NULL);

    pthread_join(th_wr.th_id, NULL);


    close_archive_file(ar);
    close(fd);
    q_destroy(in);
    q_destroy(out);
    pthread_mutex_destroy(&mut);
    free(threads);
    free(args);

    pthread_exit(NULL);
}


int main(int argc, char *argv[]) {
    struct options opt;

    opt.compress    = COMPRESS;
    opt.num_threads = 3;
    opt.size        = CHUNK_SIZE;
    opt.queue_size  = QUEUE_SIZE;
    opt.out_file    = NULL;

    read_options(argc, argv, &opt);

    if(opt.compress == COMPRESS) comp(opt);
    else decomp(opt);
}
