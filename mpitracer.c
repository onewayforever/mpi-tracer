/*
* Copyright (C) 1992-2020 HPC Product Divison, Dawning Information Industry Co., LTD.. 
*
* Author: Wan Wei (onewayforever@163.com)
*
* For detailed copyright and licensing information, please refer to the
* copyright file LICENSE.
*/

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <string.h>
#include <mpi.h>
#include <stdbool.h> 
#include <pthread.h> 
#include <sys/time.h>
#include "util.h"

static struct list_head request_pool=LIST_HEAD_INIT(request_pool);
static struct htable_node htable[MAX_HASH];


typedef int(*MPI_TYPE_COMMIT)(MPI_Datatype *datatype);
typedef int(*MPI_COMM_RANK)(MPI_Comm comm, int *rank);
typedef int(*MPI_INIT)(int *argc, char ***argv);
typedef int(*MPI_FINALIZE)();
typedef int(*MPI_SEND)(const void *buf, int count, MPI_Datatype datatype, int dest,
    int tag, MPI_Comm comm);
typedef int(*MPI_ISEND)(const void *buf, int count, MPI_Datatype datatype, int dest,
    int tag, MPI_Comm comm, MPI_Request *request);
typedef int(*MPI_RECV)(void *buf, int count, MPI_Datatype datatype,
    int source, int tag, MPI_Comm comm, MPI_Status *status);
typedef int(*MPI_IRECV)(void *buf, int count, MPI_Datatype datatype,
        int source, int tag, MPI_Comm comm, MPI_Request *request);
typedef int(*MPI_WAIT)(MPI_Request *request, MPI_Status *status);
typedef int(*MPI_WAITALL)(int count, MPI_Request array_of_requests[],
    MPI_Status *array_of_statuses);
typedef int(*MPI_TESTALL)(int count, MPI_Request array_of_requests[],
    int *flag, MPI_Status array_of_statuses[]);
typedef int(*MPI_ALLTOALL)(const void *sendbuf, int sendcount,
    MPI_Datatype sendtype, void *recvbuf, int recvcount,
    MPI_Datatype recvtype, MPI_Comm comm);

enum TRACE_TYPE{
	type_send,
	type_recv,
	type_isend,
	type_irecv,
        type_wait,
        type_waitall,
        type_testall,
        type_alltoall,
	type_COUNT
}; 
#define MAX_TRACE_NUM 100000
#define MAX_TRACE_FN 100

static char TRACE_TYPE_NAME[MAX_TRACE_FN][25]={
	"MPI_Send",
	"MPI_Recv",
	"MPI_Isend",
	"MPI_Irecv",
        "MPI_Wait",
        "MPI_Waitall",
        "MPI_Testall",
        "MPI_Alltoall",
};

static void* TRACE_TYPE_FN[MAX_TRACE_FN];

typedef struct trace_log_struct{
   int id;
   int type;
   int scount;
   int sdatatype_size;
   int rcount;
   int rdatatype_size;
   int peer;
   int tag;
   double start_ts;
   double return_ts;
   double end_ts;
   void* comm;
} trace_log_t;


static pthread_t tid;
static int writer_exit_flag=0;
static int tracer_rank=-1;
static int max_trace_num=MAX_TRACE_NUM;
static trace_log_t* probe_log=NULL;
static int trace_index=0;
static double rank_start_ts=0;
static double GHZ=-1;
static int writer_enable=1;

static double tsc_timer(){
    unsigned long var;
    unsigned int hi, lo;
    __asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
    var = ((unsigned long)hi << 32) | lo;
    double time=var/GHZ*1e-9;
    return (time);
}

static double gettimeofday_timer()
{
    struct timeval             tp;
    (void) gettimeofday( &tp, NULL );

    return ( (double) tp.tv_sec  + ((double)tp.tv_usec / 1000000.0 )) ;
}

typedef double(*Timer)();

static char log_dir[256]="/dev/shm";
static char log_prefix[256]="mpi_trace";
static void *handle = NULL;

static Timer timer_fn=gettimeofday_timer;

static inline void print_log(FILE* fp,trace_log_t* log);

void display_info(){
    printf("MPITRACER\tInject MPI to Trace Traffic\n");
    printf("MPITRACER\tLog Dir:%s, Max entries:%d\n",log_dir,max_trace_num);
    if(timer_fn==gettimeofday_timer)
        printf("MPITRACER\tUSE Timer: gettimeofday\n");
    if(timer_fn==tsc_timer)
        printf("MPITRACER\tUSE Timer: TSC with Freq:%lfGHZ\n",GHZ);
}

void* log_writer_thread(){
    char log_file[256];
    FILE* fp;
    int writer_index=0;
    int offset=0;
    while(tracer_rank<0&&!writer_exit_flag){
        sleep(1);
    }
    sprintf(log_file,"%s/%s_%d.log",log_dir,log_prefix,tracer_rank);
    fp=fopen(log_file,"w");
    fprintf( fp, "%9s %25s %11s %9s %10s %8s %7s %7s %7s %9s %8s %8s %7s %9s %8s %8s %7s\n","ID","MPI_TYPE","TimeStamp","Call","Elapse","Comm","Tag","SRC","DST","SCount","SBuf_B","SLen_B","SBW_Gbps","RCount","RBuf_B","RLen_B","RBW_Gbps");
    while(!writer_exit_flag){
        offset = trace_index%max_trace_num;
        if(writer_index==offset){
           sleep(1);
           continue;
        }
        print_log(fp,&probe_log[writer_index]);
        writer_index=(writer_index+1)%max_trace_num;
    }
    fclose(fp);
}

int MPI_Init(int *argc, char ***argv){
    static MPI_INIT old_fn= NULL;
    int ret=0;
    int i;
    char* env;
    handle = dlopen("libmpi.so", RTLD_LAZY);
    old_fn= (MPI_INIT)dlsym(handle, "MPI_Init");
    init_request_pool(&request_pool,1024);
    init_htable(htable);
    env=getenv("MPITRACER_LOG_SIZE");
    if(!env){
        max_trace_num=MAX_TRACE_NUM;
    }else{
        max_trace_num=atoi(env)>0 ? atoi(env):MAX_TRACE_NUM;
    }
    env=getenv("MPITRACER_TSC_GHZ");
    if(!env){
        GHZ=-1;
    }else{
        GHZ=atof(env);
    }
    env=getenv("MPITRACER_TIMER");
    if(env){
        if(strcmp(env,"GETTIMEOFDAY")==0){
            timer_fn=gettimeofday_timer;
        }
        if(strcmp(env,"TSC")==0){
            timer_fn=tsc_timer;
            if(GHZ<=0||GHZ>=10){
                printf("MPITRACER ERROR: You need set correct envrionment MPITRACER_TSC_GHZ to use TSC\n");
                exit(0);
            }
        }
    }
    env=getenv("MPITRACER_LOG_DIR");
    if(env){
        sprintf(log_dir,"%s",env);
    }
    
    env=getenv("MPITRACER_LOG_PREFIX");
    if(env){
        sprintf(log_prefix,"%s",env);
    }
    
    env=getenv("MPITRACER_DELAY_WRITER");
    if(env){
        if(atoi(env)>0){
            writer_enable=0; 
        }
    }

    probe_log=(trace_log_t*)malloc(sizeof(trace_log_t)*max_trace_num);
    for(i=0;i<max_trace_num;i++){
        probe_log[i].type=-1;
    }
    memset(TRACE_TYPE_FN,0,sizeof(void*)*MAX_TRACE_FN);
    for(i=0;i<type_COUNT;i++){
        TRACE_TYPE_FN[i] = dlsym(handle, TRACE_TYPE_NAME[i]);
    }
    rank_start_ts=timer_fn();
    if(writer_enable){
        if(pthread_create(&tid, NULL, log_writer_thread, NULL)<0){
            printf("MPITRACER: create writer thread fail!\n");
            exit(0);
        }
    }
    ret=old_fn(argc, argv);
    return ret;
}


inline void print_log(FILE* fp,trace_log_t* log){
    int src=-1;
    int dst=-1;
    int slen=0;
    int rlen=0;
    int ssize=0;
    int rsize=0;
    double sgbps=0.0;
    double rgbps=0.0;
    double elapse=0.0;
    switch(log->type){
        case type_send:
        case type_isend:
            src=tracer_rank;
            dst=log->peer;
            break;
        case type_recv:
        case type_irecv:
            dst=tracer_rank;
            src=log->peer;
            break;
        case -1:
            return;
        default:
            break;
    }
    elapse=log->end_ts-log->start_ts;
    ssize=log->sdatatype_size;
    rsize=log->rdatatype_size;
    slen=ssize*log->scount;
    if(slen>0&&elapse>0){
        sgbps=slen*8/elapse*1e-9;
    }else{
        sgbps=0.0;
    }
    rlen=rsize*log->rcount;
    if(rlen>0&&elapse>0){
        rgbps=rlen*8/elapse*1e-9;
    }else{
        rgbps=0.0;
    }
    fprintf( fp, "%9d %25s %11.6lf %9.6lf %10.6lf %8u %7d %7d %7d %9d %8d %8d %7.3lf %9d %8d %8d %7.3lf\n", log->id,TRACE_TYPE_NAME[log->type],(log->start_ts-rank_start_ts) ,(log->return_ts-log->start_ts),elapse,log->comm,log->tag,src,dst,log->scount,log->sdatatype_size,slen,sgbps,log->rcount,log->rdatatype_size,rlen,rgbps);
}

int MPI_Finalize(){
    static MPI_FINALIZE old_fn= NULL;
    int ret=0;
    char log_file[256];
    FILE* fp;
    int i,offset;
    old_fn= (MPI_FINALIZE)dlsym(handle, "MPI_Finalize");
    ret=old_fn();
    if(writer_enable){
        writer_exit_flag=1;
        pthread_join(tid,NULL);
    }else{
        if(0==tracer_rank){
            printf("MPITRACER\tRecording MPI Hook of rank:%d,total entries:%d\n",tracer_rank,trace_index);
        }
        sprintf(log_file,"%s/%s_%d.log",log_dir,log_prefix,tracer_rank);
        fp=fopen(log_file,"w");
        fprintf( fp, "%9s %25s %11s %9s %10s %8s %7s %7s %7s %9s %8s %8s %7s %9s %8s %8s %7s\n","ID","MPI_TYPE","TimeStamp","Call","Elapse","Comm","Tag","SRC","DST","SCount","SBuf_B","SLen_B","SBW_Gbps","RCount","RBuf_B","RLen_B","RBW_Gbps");
        offset = trace_index%max_trace_num;
        for(i=offset;i<max_trace_num;i++){
            print_log(fp,&probe_log[i]);
        }
        for(i=0;i<offset;i++){
            print_log(fp,&probe_log[i]);
        }
        fclose(fp);
    }
    free(probe_log);
    if(0==tracer_rank){
        printf("MPITRACER\tPlease check files of %s/%s_<rank_id>.log on each node\n",log_dir,log_prefix);
        sleep(3);
    }
    return ret;
}

int MPI_Comm_rank(MPI_Comm comm, int *rank){
    static MPI_COMM_RANK old_fn= NULL;
    int ret=0;
    old_fn= (MPI_COMM_RANK)dlsym(handle, "MPI_Comm_rank");
    ret=old_fn(comm, rank);
    if(tracer_rank==-1){
        tracer_rank=*rank;
        if(0==tracer_rank){
            display_info();
        }
    }
    return ret;
}

int MPI_Send(const void *buf, int count, MPI_Datatype datatype, int dest,
    int tag, MPI_Comm comm){
    int ret;
    int type=type_send;
    double end;
    MPI_SEND fn=TRACE_TYPE_FN[type];
    probe_log[trace_index%max_trace_num].type=type;
    probe_log[trace_index%max_trace_num].start_ts=timer_fn();
    ret=fn(buf,count,datatype,dest,tag,comm);
    end=timer_fn();
    probe_log[trace_index%max_trace_num].comm=comm;
    probe_log[trace_index%max_trace_num].end_ts=end;
    probe_log[trace_index%max_trace_num].return_ts=end;
    probe_log[trace_index%max_trace_num].id=trace_index;
    probe_log[trace_index%max_trace_num].peer=dest;
    probe_log[trace_index%max_trace_num].scount=count;
    MPI_Type_size(datatype,&probe_log[trace_index%max_trace_num].sdatatype_size);
    probe_log[trace_index%max_trace_num].rcount=0;
    probe_log[trace_index%max_trace_num].rdatatype_size=0;
    probe_log[trace_index%max_trace_num].tag=tag;
    trace_index++;
    return ret;
}

int MPI_Isend(const void *buf, int count, MPI_Datatype datatype, int dest,
    int tag, MPI_Comm comm, MPI_Request *request){
    int ret;
    int type=type_isend;
    double end;
    struct request_node* r;
    MPI_ISEND fn=TRACE_TYPE_FN[type];
    probe_log[trace_index%max_trace_num].type=type;
    probe_log[trace_index%max_trace_num].start_ts=timer_fn();
    ret=fn(buf,count,datatype,dest,tag,comm,request);
    end=timer_fn();
    probe_log[trace_index%max_trace_num].comm=comm;
    probe_log[trace_index%max_trace_num].end_ts=end+999.0;
    probe_log[trace_index%max_trace_num].return_ts=end;
    probe_log[trace_index%max_trace_num].id=trace_index;
    probe_log[trace_index%max_trace_num].peer=dest;
    probe_log[trace_index%max_trace_num].scount=count;
    MPI_Type_size(datatype,&probe_log[trace_index%max_trace_num].sdatatype_size);
    probe_log[trace_index%max_trace_num].rcount=0;
    probe_log[trace_index%max_trace_num].rdatatype_size=0;
    probe_log[trace_index%max_trace_num].tag=tag;
    r=pool_pop(&request_pool);
    if(r){
        r->ptr=(void*)request;
        r->index=trace_index;
        hash_add_request(r,htable);
    }
    trace_index++;
    return ret;
}

int MPI_Recv(void *buf, int count, MPI_Datatype datatype,
    int source, int tag, MPI_Comm comm, MPI_Status *status){
    int ret;
    int type=type_recv;
    double end;
    MPI_RECV fn=TRACE_TYPE_FN[type];
    probe_log[trace_index%max_trace_num].type=type;
    probe_log[trace_index%max_trace_num].start_ts=timer_fn();
    ret=fn(buf,count,datatype,source,tag,comm,status);
    end=timer_fn();
    probe_log[trace_index%max_trace_num].comm=comm;
    probe_log[trace_index%max_trace_num].end_ts=end;
    probe_log[trace_index%max_trace_num].return_ts=end;
    probe_log[trace_index%max_trace_num].id=trace_index;
    probe_log[trace_index%max_trace_num].peer=source;
    probe_log[trace_index%max_trace_num].scount=0;
    probe_log[trace_index%max_trace_num].sdatatype_size=0;
    probe_log[trace_index%max_trace_num].rcount=count;
    MPI_Type_size(datatype,&probe_log[trace_index%max_trace_num].rdatatype_size);
    probe_log[trace_index%max_trace_num].tag=tag;
    trace_index++;
    return ret;
}

int MPI_Irecv(void *buf, int count, MPI_Datatype datatype,
        int source, int tag, MPI_Comm comm, MPI_Request *request){
    int ret;
    int type=type_irecv;
    double end;
    struct request_node* r;
    MPI_IRECV fn=TRACE_TYPE_FN[type];
    probe_log[trace_index%max_trace_num].type=type;
    probe_log[trace_index%max_trace_num].start_ts=timer_fn();
    ret=fn(buf,count,datatype,source,tag,comm,request);
    end=timer_fn();
    probe_log[trace_index%max_trace_num].comm=comm;
    probe_log[trace_index%max_trace_num].end_ts=end+999.0;
    probe_log[trace_index%max_trace_num].return_ts=end;
    probe_log[trace_index%max_trace_num].id=trace_index;
    probe_log[trace_index%max_trace_num].peer=source;
    probe_log[trace_index%max_trace_num].scount=0;
    probe_log[trace_index%max_trace_num].sdatatype_size=0;
    probe_log[trace_index%max_trace_num].rcount=count;
    MPI_Type_size(datatype,&probe_log[trace_index%max_trace_num].rdatatype_size);
    probe_log[trace_index%max_trace_num].tag=tag;
    r=pool_pop(&request_pool);
    if(r){
        r->ptr=(void*)request;
        r->index=trace_index;
        hash_add_request(r,htable);
    }
    trace_index++;
    return ret;
}

int MPI_Wait(MPI_Request *request, MPI_Status *status){
    int ret;
    int i;
    int type=type_wait;
    struct request_node* r;
    double start,end;
    void* ptr;
    MPI_WAIT fn=TRACE_TYPE_FN[type];
    probe_log[trace_index%max_trace_num].type=type;
    probe_log[trace_index%max_trace_num].start_ts=timer_fn();
    ret=fn(request,status);
    end=timer_fn();
    ptr=(void*)((MPI_Request*)request); 
    r=hash_find_request(ptr,htable);
    if(r){
        probe_log[r->index%max_trace_num].end_ts=end;
        recycle_request_node(r,&request_pool);
    } 
    probe_log[trace_index%max_trace_num].comm=0;
    probe_log[trace_index%max_trace_num].end_ts=end;
    probe_log[trace_index%max_trace_num].return_ts=end;
    probe_log[trace_index%max_trace_num].id=trace_index;
    probe_log[trace_index%max_trace_num].peer=-1;
    probe_log[trace_index%max_trace_num].scount=1;
    probe_log[trace_index%max_trace_num].sdatatype_size=0;
    probe_log[trace_index%max_trace_num].rcount=0;
    probe_log[trace_index%max_trace_num].rdatatype_size=0;
    probe_log[trace_index%max_trace_num].tag=-1;
    trace_index++;
    return ret;
}

int MPI_Waitall(int count, MPI_Request array_of_requests[],
    MPI_Status *array_of_statuses){
    int ret;
    int i;
    int type=type_waitall;
    struct request_node* r;
    double start,end;
    void* ptr;
    MPI_WAITALL fn=TRACE_TYPE_FN[type];
    probe_log[trace_index%max_trace_num].type=type;
    probe_log[trace_index%max_trace_num].start_ts=timer_fn();
    ret=fn(count,array_of_requests,array_of_statuses);
    end=timer_fn();
    for(i=0;i<count;i++){
       ptr=(void*)((MPI_Request*)array_of_requests+i); 
       r=hash_find_request(ptr,htable);
       if(!r){
          continue;
       }else{
          probe_log[r->index%max_trace_num].end_ts=end;
          recycle_request_node(r,&request_pool);
       } 
    }
    probe_log[trace_index%max_trace_num].comm=0;
    probe_log[trace_index%max_trace_num].end_ts=end;
    probe_log[trace_index%max_trace_num].return_ts=end;
    probe_log[trace_index%max_trace_num].id=trace_index;
    probe_log[trace_index%max_trace_num].peer=-1;
    probe_log[trace_index%max_trace_num].scount=count;
    probe_log[trace_index%max_trace_num].sdatatype_size=0;
    probe_log[trace_index%max_trace_num].rcount=0;
    probe_log[trace_index%max_trace_num].rdatatype_size=0;
    probe_log[trace_index%max_trace_num].tag=-1;
    trace_index++;
    return ret;
}

int MPI_Testall(int count, MPI_Request array_of_requests[],
    int *flag, MPI_Status array_of_statuses[]){
    int ret;
    int i;
    int type=type_testall;
    struct request_node* r;
    double start,end;
    void* ptr;
    MPI_TESTALL fn=TRACE_TYPE_FN[type];
    ret=fn(count,array_of_requests,flag,array_of_statuses);
    if(*flag==false){
        return ret;
    }
    probe_log[trace_index%max_trace_num].type=type;
    probe_log[trace_index%max_trace_num].start_ts=timer_fn();
    end=timer_fn();
    for(i=0;i<count;i++){
       ptr=(void*)((MPI_Request*)array_of_requests+i); 
       r=hash_find_request(ptr,htable);
       if(!r){
          continue;
       }else{
          probe_log[r->index%max_trace_num].end_ts=end;
          recycle_request_node(r,&request_pool);
       } 
    }
    probe_log[trace_index%max_trace_num].comm=0;
    probe_log[trace_index%max_trace_num].end_ts=end;
    probe_log[trace_index%max_trace_num].return_ts=end;
    probe_log[trace_index%max_trace_num].id=trace_index;
    probe_log[trace_index%max_trace_num].peer=-1;
    probe_log[trace_index%max_trace_num].scount=count;
    probe_log[trace_index%max_trace_num].sdatatype_size=0;
    probe_log[trace_index%max_trace_num].rcount=0;
    probe_log[trace_index%max_trace_num].rdatatype_size=0;
    probe_log[trace_index%max_trace_num].tag=-1;
    trace_index++;
    return ret;
}

int MPI_Alltoall(const void *sendbuf, int sendcount,
    MPI_Datatype sendtype, void *recvbuf, int recvcount,
    MPI_Datatype recvtype, MPI_Comm comm){
    int ret;
    int type=type_alltoall;
    double end;
    MPI_ALLTOALL fn=TRACE_TYPE_FN[type];
    probe_log[trace_index%max_trace_num].type=type;
    probe_log[trace_index%max_trace_num].start_ts=timer_fn();
    ret=fn(sendbuf,sendcount,sendtype,recvbuf,recvcount,recvtype,comm);
    end=timer_fn();
    probe_log[trace_index%max_trace_num].comm=comm;
    probe_log[trace_index%max_trace_num].end_ts=end;
    probe_log[trace_index%max_trace_num].return_ts=end;
    probe_log[trace_index%max_trace_num].id=trace_index;
    probe_log[trace_index%max_trace_num].peer=-1;
    probe_log[trace_index%max_trace_num].scount=sendcount;
    MPI_Type_size(sendtype,&probe_log[trace_index%max_trace_num].sdatatype_size);
    probe_log[trace_index%max_trace_num].rcount=recvcount;
    MPI_Type_size(recvtype,&probe_log[trace_index%max_trace_num].rdatatype_size);
    probe_log[trace_index%max_trace_num].tag=-1;
    trace_index++;
    return ret;
}
