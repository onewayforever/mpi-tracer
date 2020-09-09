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


#define MT

#ifdef MT
#define MT_LOCK() {if(mt_flag) { pthread_mutex_lock(&log_mutex);}}
#define MT_UNLOCK() {if(mt_flag) { pthread_mutex_unlock(&log_mutex);}}
#else
#define MT_LOCK() {}
#define MT_UNLOCK() {}
#endif


#define END_TS_DELAY 0

static struct list_head request_pool=LIST_HEAD_INIT(request_pool);
static struct htable_node htable[MAX_HASH];


typedef int(*MPI_TYPE_COMMIT)(MPI_Datatype *datatype);
typedef int(*MPI_COMM_RANK)(MPI_Comm comm, int *rank);
typedef int(*MPI_INIT)(int *argc, char ***argv);
typedef int(*MPI_INIT_THREAD)(int *argc, char ***argv,
    int required, int *provided);
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
typedef int(*MPI_TEST)(MPI_Request *request, int *flag, MPI_Status *status);
typedef int(*MPI_TESTALL)(int count, MPI_Request array_of_requests[],
    int *flag, MPI_Status array_of_statuses[]);
typedef int(*MPI_ALLTOALL)(const void *sendbuf, int sendcount,
    MPI_Datatype sendtype, void *recvbuf, int recvcount,
    MPI_Datatype recvtype, MPI_Comm comm);
typedef int(*MPI_BCAST)(void *buffer, int count, MPI_Datatype datatype,
    int root, MPI_Comm comm);
typedef int(*MPI_IBCAST)(void *buffer, int count, MPI_Datatype datatype,
    int root, MPI_Comm comm, MPI_Request *request);
typedef int(*MPI_REDUCE)(const void *sendbuf, void *recvbuf, int count,
               MPI_Datatype datatype, MPI_Op op, int root,
               MPI_Comm comm);
typedef int(*MPI_IREDUCE)(const void *sendbuf, void *recvbuf, int count,
                MPI_Datatype datatype, MPI_Op op, int root,
                MPI_Comm comm, MPI_Request *request);
typedef int(*MPI_ALLREDUCE)(const void *sendbuf, void *recvbuf, int count,
                  MPI_Datatype datatype, MPI_Op op, MPI_Comm comm);
typedef int(*MPI_IALLREDUCE)(const void *sendbuf, void *recvbuf, int count,
                   MPI_Datatype datatype, MPI_Op op, MPI_Comm comm,
                   MPI_Request *request);
typedef int(*MPI_IALLTOALL)(const void *sendbuf, int sendcount,
    MPI_Datatype sendtype, void *recvbuf, int recvcount,
    MPI_Datatype recvtype, MPI_Comm comm, MPI_Request *request);
typedef int(*MPI_BARRIER)(MPI_Comm comm);
typedef int(*MPI_IBARRIER)(MPI_Comm comm, MPI_Request *request);

enum TRACE_TYPE{
	type_send,
	type_recv,
	type_isend,
	type_irecv,
        type_wait,
        type_waitall,
        type_test,
        type_testall,
        type_alltoall,
        type_ialltoall,
        type_bcast,
        type_ibcast,
        type_reduce,
        type_ireduce,
        type_allreduce,
        type_iallreduce,
        type_barrier,
        type_ibarrier,
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
        "MPI_Test",
        "MPI_Testall",
        "MPI_Alltoall",
        "MPI_Ialltoall",
        "MPI_Bcast",
        "MPI_Ibcast",
        "MPI_Reduce",
        "MPI_Ireduce",
        "MPI_Allreduce",
        "MPI_Iallreduce",
        "MPI_Barrier",
        "MPI_Ibarrier",
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
   /*long private;*/
} trace_log_t;


static pthread_t tid;
pthread_mutex_t log_mutex;
static int tracer_init_flag=0;
static int writer_exit_flag=0;
static int mt_flag=0;
static int tracer_rank=-1;
static int max_trace_num=MAX_TRACE_NUM;
static trace_log_t* probe_log=NULL;
static int trace_index=0;
static double rank_start_ts=0;
static double GHZ=-1;
static int writer_enable=1;
static int log_threshold=0;

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
    printf("MPITRACER\tLog file:%s/%s_<rankid>.log, Max entries:%d\n",log_dir,log_prefix,max_trace_num);
    if(timer_fn==gettimeofday_timer)
        printf("MPITRACER\tUse Timer: gettimeofday\n");
    if(timer_fn==tsc_timer)
        printf("MPITRACER\tUse Timer: TSC with Freq:%lfGHZ\n",GHZ);
    if(writer_enable){
        printf("MPITRACER\tUse Realtime Writer thread to log\n");
    }else{
        printf("MPITRACER\tSave Logs at the end of MPI_Finalize, DO NOT EXIT EARLY\n");
    }
    if(mt_flag){
        printf("MPITRACER\tMPI_THREAD_MULTIPLE mode detected\n");
    }
    if(log_threshold>0){
        printf("MPITRACER\tMPITRACER_THRESHOLD=%d, only message larger than this value will be logged\n",log_threshold);
    }
}

#define MAX_WAIT_DELAY 1000
void* log_writer_thread(){
    char log_file[256];
    FILE* fp;
    int writer_index=0;
    int offset=0;
    int n=0;
    int wait_n=0;
    while(tracer_rank<0&&!writer_exit_flag){
        usleep(1);
    }
    sprintf(log_file,"%s/%s_%d.log",log_dir,log_prefix,tracer_rank);
    fp=fopen(log_file,"w");
    fprintf( fp, "%9s %25s %11s %9s %10s %8s %7s %7s %7s %9s %8s %8s %7s %9s %8s %8s %7s\n","ID","MPI_TYPE","TimeStamp","Call","Elapse","Comm","Tag","SRC","DST","SCount","SBuf_B","SLen_B","SBW_Gbps","RCount","RBuf_B","RLen_B","RBW_Gbps");
    while(1){
        offset = trace_index%max_trace_num;
        if(writer_index==offset){
            if(writer_exit_flag){
                break;
            }
            usleep(100000);
            continue;
        }
        if(probe_log[writer_index].end_ts==END_TS_DELAY){
            /* writer may be too fast*/
            if(wait_n < MAX_WAIT_DELAY){
                usleep(1000);
                wait_n++;
                continue;
            }else{
                printf("why?\n");
            }
        }
        wait_n=0;
        print_log(fp,&probe_log[writer_index]);
        writer_index=(writer_index+1)%max_trace_num;
        n++;
    }
    fclose(fp);
    return NULL;
}

void init_mpitracer(){
    int i;
    char* env;
    if(tracer_init_flag) return;
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
    
    env=getenv("MPITRACER_THRESHOLD");
    if(env){
        if(atoi(env)>0){
            log_threshold=atoi(env); 
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
    if(mt_flag){
        pthread_mutex_init( &log_mutex, NULL ) ;
    }
    tracer_init_flag=1;
}

int MPI_Init(int *argc, char ***argv){
    static MPI_INIT old_fn= NULL;
    int ret=0;
    handle = dlopen("libmpi.so", RTLD_LAZY);
    old_fn= (MPI_INIT)dlsym(handle, "MPI_Init");
    init_mpitracer();
    ret=old_fn(argc, argv);
    return ret;
}

int MPI_Init_thread(int *argc, char ***argv,
    int required, int *provided){
    static MPI_INIT_THREAD old_fn= NULL;
    int ret=0;
    handle = dlopen("libmpi.so", RTLD_LAZY);
    old_fn= (MPI_INIT_THREAD)dlsym(handle, "MPI_Init_thread");
    if(MPI_THREAD_MULTIPLE==required) {
        mt_flag=1;
    }
    init_mpitracer();
    ret=old_fn(argc, argv,required,provided);
    return ret;
}


inline void print_log(FILE* fp,trace_log_t* log){
    void* comm=log->comm;
    int src=-1;
    int dst=-1;
    int slen=0;
    int rlen=0;
    int ssize=log->sdatatype_size;
    int rsize=log->rdatatype_size;
    double sgbps=0.0;
    double rgbps=0.0;
    double elapse=0.0;
    char debug_info[16]="";
    int tag = -1;
    int scount=log->scount;
    int rcount=log->rcount;
    switch(log->type){
        case type_send:
        case type_isend:
            src=tracer_rank;
            dst=log->peer;
            tag=log->tag;
            rcount=0;
            rsize=0;
            break;
        case type_recv:
        case type_irecv:
            dst=tracer_rank;
            src=log->peer;
            tag=log->tag;
            scount=0;
            ssize=0;
            break;
        case type_bcast:
        case type_ibcast:
            src=log->peer;
            break;
        case type_reduce:
        case type_ireduce:
            dst=log->peer;
            break;
        case type_wait:
        case type_test:
        case type_waitall:
        case type_testall:
            comm=0;
            rcount=0;
            ssize=0;
            rsize=0;
            break;
        case type_barrier:
        case type_ibarrier:
            scount=0;
            rcount=0;
            ssize=0;
            rsize=0;
            break;
        case -1:
            return;
        default:
            break;
    }
    elapse=log->end_ts==0? 999.0:(log->end_ts-log->start_ts);
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
    fprintf( fp, "%9d %25s %11.6lf %9.6lf %10.6lf %10p %7d %7d %7d %9d %8d %8d %7.3lf %9d %8d %8d %7.3lf %s\n", log->id,TRACE_TYPE_NAME[log->type],(log->start_ts-rank_start_ts) ,(log->return_ts-log->start_ts),elapse,comm,tag,src,dst,scount,ssize,slen,sgbps,rcount,rsize,rlen,rgbps,debug_info);
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
        fprintf( fp, "%9s %25s %11s %9s %10s %10s %7s %7s %7s %9s %8s %8s %7s %9s %8s %8s %7s\n","ID","MPI_TYPE","TimeStamp","Call","Elapse","Comm","Tag","SRC","DST","SCount","SBuf_B","SLen_B","SBW_Gbps","RCount","RBuf_B","RLen_B","RBW_Gbps");
        offset = trace_index%max_trace_num;
        for(i=offset;i<max_trace_num;i++){
            print_log(fp,&probe_log[i]);
        }
        for(i=0;i<offset;i++){
            print_log(fp,&probe_log[i]);
        }
        fclose(fp);
    }
    if(mt_flag){
        pthread_mutex_destroy( &log_mutex ) ;
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

inline void new_log(trace_log_t* log,int type,int idx,double start_ts,double return_ts,double end_ts){
    log->type=type;
    log->id=idx;
    log->start_ts=start_ts;
    log->return_ts=return_ts;
    log->end_ts=end_ts;
}

inline static void cache_request(MPI_Request* request,int index){
    struct request_node* r;
    r=pool_pop(&request_pool);
    if(r){
        r->ptr=(void*)request;
        r->index=index;
        hash_add_request(r,htable);
    }
}

int MPI_Send(const void *buf, int count, MPI_Datatype datatype, int dest,
    int tag, MPI_Comm comm){
    int ret;
    int type=type_send;
    double start,end;
    int idx=-1;
    trace_log_t* log;
    MPI_SEND fn=TRACE_TYPE_FN[type];
    int size=0;
    MPI_Type_size(datatype,&size);
    if(log_threshold>0&&log_threshold>count*size) return fn(buf,count,datatype,dest,tag,comm);
    start=timer_fn();
    ret=fn(buf,count,datatype,dest,tag,comm);
    end=timer_fn();
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,end);
    log->comm=comm;
    log->peer=dest;
    log->scount=count;
    log->sdatatype_size=size;
    log->tag=tag;
    trace_index++;
    MT_UNLOCK()
    return ret;
}

int MPI_Isend(const void *buf, int count, MPI_Datatype datatype, int dest,
    int tag, MPI_Comm comm, MPI_Request *request){
    int ret;
    int type=type_isend;
    double start,end;
    int idx=-1;
    trace_log_t* log;
    MPI_ISEND fn=TRACE_TYPE_FN[type];
    int size=0;
    MPI_Type_size(datatype,&size);
    if(log_threshold>0&&log_threshold>count*size) return fn(buf,count,datatype,dest,tag,comm,request);
    start=timer_fn();
    ret=fn(buf,count,datatype,dest,tag,comm,request);
    end=timer_fn();
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,0);
    log->comm=comm;
    log->peer=dest;
    log->scount=count;
    log->sdatatype_size=size;
    log->tag=tag;
    cache_request(request,trace_index);
    trace_index++;
    MT_UNLOCK()
    return ret;
}

int MPI_Recv(void *buf, int count, MPI_Datatype datatype,
    int source, int tag, MPI_Comm comm, MPI_Status *status){
    int ret;
    int type=type_recv;
    double start,end;
    int idx=-1;
    trace_log_t* log;
    MPI_RECV fn=TRACE_TYPE_FN[type];
    int size=0;
    MPI_Type_size(datatype,&size);
    if(log_threshold>0&&log_threshold>count*size) return fn(buf,count,datatype,source,tag,comm,status);
    start=timer_fn();
    ret=fn(buf,count,datatype,source,tag,comm,status);
    end=timer_fn();
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,end);
    log->comm=comm;
    log->peer=source;
    log->rcount=count;
    log->rdatatype_size=size;
    log->tag=tag;
    trace_index++;
    MT_UNLOCK()
    return ret;
}

int MPI_Irecv(void *buf, int count, MPI_Datatype datatype,
        int source, int tag, MPI_Comm comm, MPI_Request *request){
    int ret;
    int type=type_irecv;
    double start,end;
    int idx=-1;
    trace_log_t* log;
    MPI_IRECV fn=TRACE_TYPE_FN[type];
    int size=0;
    MPI_Type_size(datatype,&size);
    if(log_threshold>0&&log_threshold>count*size) return fn(buf,count,datatype,source,tag,comm,request);
    start=timer_fn();
    ret=fn(buf,count,datatype,source,tag,comm,request);
    end=timer_fn();
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,0);
    log->comm=comm;
    log->peer=source;
    log->rcount=count;
    log->rdatatype_size=size;
    log->tag=tag;
    cache_request(request,trace_index);
    trace_index++;
    MT_UNLOCK()
    return ret;
}

inline static void update_end_ts_of_cached_logs(int count,MPI_Request* array_of_requests,double end){
    int i;
    void* ptr;
    struct request_node* r;
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
}

int MPI_Wait(MPI_Request *request, MPI_Status *status){
    int ret;
    int type=type_wait;
    double start,end;
    int idx=-1;
    trace_log_t* log;
    MPI_WAIT fn=TRACE_TYPE_FN[type];
    start=timer_fn();
    ret=fn(request,status);
    end=timer_fn();
    update_end_ts_of_cached_logs(1,request,end);
    if(log_threshold>0) return ret;
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,end);
    log->scount=1;
    trace_index++;
    MT_UNLOCK()
    return ret;
}

int MPI_Waitall(int count, MPI_Request array_of_requests[],
    MPI_Status *array_of_statuses){
    int ret;
    int idx=-1;
    int type=type_waitall;
    double start,end;
    trace_log_t* log;
    MPI_WAITALL fn=TRACE_TYPE_FN[type];
    start=timer_fn();
    ret=fn(count,array_of_requests,array_of_statuses);
    end=timer_fn();
    update_end_ts_of_cached_logs(count,array_of_requests,end);
    if(log_threshold>0) return ret;
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,end);
    log->scount=count;
    trace_index++;
    MT_UNLOCK()
    return ret;
}

int MPI_Test(MPI_Request *request, int *flag, MPI_Status *status){
    int ret;
    int type=type_test;
    double start,end;
    int idx=-1;
    trace_log_t* log;
    MPI_TEST fn=TRACE_TYPE_FN[type];
    start=timer_fn();
    ret=fn(request,flag,status);
    if(*flag==false){
        return ret;
    }
    end=timer_fn();
    update_end_ts_of_cached_logs(1,request,end);
    if(log_threshold>0) return ret;
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,end);
    log->scount=1;
    trace_index++;
    MT_UNLOCK()
    return ret;

}

int MPI_Testall(int count, MPI_Request array_of_requests[],
    int *flag, MPI_Status array_of_statuses[]){
    int ret;
    int type=type_testall;
    double start,end;
    int idx=-1;
    trace_log_t* log;
    MPI_TESTALL fn=TRACE_TYPE_FN[type];
    start=timer_fn();
    ret=fn(count,array_of_requests,flag,array_of_statuses);
    if(*flag==false){
        return ret;
    }
    end=timer_fn();
    update_end_ts_of_cached_logs(count,array_of_requests,end);
    if(log_threshold>0) return ret;
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,end);
    log->scount=count;
    trace_index++;
    MT_UNLOCK()
    return ret;
}

int MPI_Alltoall(const void *sendbuf, int sendcount,
    MPI_Datatype sendtype, void *recvbuf, int recvcount,
    MPI_Datatype recvtype, MPI_Comm comm){
    int ret;
    int idx=-1;
    int type=type_alltoall;
    double start,end;
    trace_log_t* log;
    MPI_ALLTOALL fn=TRACE_TYPE_FN[type];
    int ssize=0;
    int rsize=0;
    MPI_Type_size(sendtype,&ssize);
    MPI_Type_size(recvtype,&rsize);
    if(log_threshold>0&&(log_threshold>recvcount*rsize||log_threshold>sendcount*ssize)) return ret=fn(sendbuf,sendcount,sendtype,recvbuf,recvcount,recvtype,comm);
    start=timer_fn();
    ret=fn(sendbuf,sendcount,sendtype,recvbuf,recvcount,recvtype,comm);
    end=timer_fn();
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,end);
    log->comm=comm;
    log->scount=sendcount;
    log->sdatatype_size=ssize;
    log->rcount=recvcount;
    log->rdatatype_size=rsize;
    trace_index++;
    MT_UNLOCK()
    return ret;
}

int MPI_Ialltoall(const void *sendbuf, int sendcount,
    MPI_Datatype sendtype, void *recvbuf, int recvcount,
    MPI_Datatype recvtype, MPI_Comm comm, MPI_Request *request){
    int ret;
    int idx=-1;
    int type=type_ialltoall;
    double start,end;
    trace_log_t* log;
    MPI_IALLTOALL fn=TRACE_TYPE_FN[type];
    int ssize=0;
    int rsize=0;
    MPI_Type_size(sendtype,&ssize);
    MPI_Type_size(recvtype,&rsize);
    if(log_threshold>0&&(log_threshold>recvcount*rsize||log_threshold>sendcount*ssize)) return ret=fn(sendbuf,sendcount,sendtype,recvbuf,recvcount,recvtype,comm,request);
    start=timer_fn();
    ret=fn(sendbuf,sendcount,sendtype,recvbuf,recvcount,recvtype,comm,request);
    end=timer_fn();
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,0);
    log->comm=comm;
    log->scount=sendcount;
    log->sdatatype_size=ssize;
    log->rcount=recvcount;
    log->rdatatype_size=rsize;
    cache_request(request,trace_index);
    trace_index++;
    MT_UNLOCK()
    return ret;
}

int MPI_Bcast(void *buffer, int count, MPI_Datatype datatype,
    int root, MPI_Comm comm){
    int idx=-1;
    int ret;
    int type=type_bcast;
    double start,end;
    trace_log_t* log;
    MPI_BCAST fn=TRACE_TYPE_FN[type];
    int size=0;
    MPI_Type_size(datatype,&size);
    if(log_threshold>0&&log_threshold>count*size) return fn(buffer,count,datatype,root,comm);
    start=timer_fn();
    ret=fn(buffer,count,datatype,root,comm);
    end=timer_fn();
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,end);
    log->comm=comm;
    log->peer=root;
    if(root==tracer_rank){
        log->scount=count;
        log->sdatatype_size=size;
        log->rcount=0;
        log->rdatatype_size=0;
    }else{
        log->rcount=count;
        log->rdatatype_size=size;
        log->scount=0;
        log->sdatatype_size=0;
    }
    trace_index++;
    MT_UNLOCK()
    return ret;
}

int MPI_Ibcast(void *buffer, int count, MPI_Datatype datatype,
    int root, MPI_Comm comm, MPI_Request *request){
    int ret;
    int type=type_ibcast;
    int idx=-1;
    double start,end;
    trace_log_t* log;
    MPI_IBCAST fn=TRACE_TYPE_FN[type];
    int size=0;
    MPI_Type_size(datatype,&size);
    if(log_threshold>0&&log_threshold>count*size) return fn(buffer,count,datatype,root,comm,request);
    start=timer_fn();
    ret=fn(buffer,count,datatype,root,comm,request);
    end=timer_fn();
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,end);
    log->comm=comm;
    log->peer=root;
    if(root==tracer_rank){
        log->scount=count;
        log->sdatatype_size=size;
        log->rcount=0;
        log->rdatatype_size=0;
    }else{
        log->rcount=count;
        log->rdatatype_size=size;
        log->scount=0;
        log->sdatatype_size=0;
    }
    cache_request(request,trace_index);
    trace_index++;
    MT_UNLOCK()
    return ret;
}

int MPI_Reduce(const void *sendbuf, void *recvbuf, int count,
               MPI_Datatype datatype, MPI_Op op, int root,
               MPI_Comm comm){
    int ret;
    int type=type_reduce;
    int idx=-1;
    double start,end;
    trace_log_t* log;
    MPI_REDUCE fn=TRACE_TYPE_FN[type];
    int size=0;
    MPI_Type_size(datatype,&size);
    if(log_threshold>0&&log_threshold>count*size) return fn(sendbuf,recvbuf,count,datatype,op,root,comm);
    start=timer_fn();
    ret=fn(sendbuf,recvbuf,count,datatype,op,root,comm);
    end=timer_fn();
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,end);
    log->comm=comm;
    log->peer=root;
    if(root==tracer_rank){
        log->rcount=count;
        log->rdatatype_size=size;
        log->scount=0;
        log->sdatatype_size=0;
    }else{
        log->scount=count;
        log->sdatatype_size=size;
        log->rcount=0;
        log->rdatatype_size=0;
    }
    trace_index++;
    MT_UNLOCK()
    return ret;
}

int MPI_Ireduce(const void *sendbuf, void *recvbuf, int count,
                MPI_Datatype datatype, MPI_Op op, int root,
                MPI_Comm comm, MPI_Request *request){
    int ret;
    int type=type_ireduce;
    int idx=-1;
    double start,end;
    trace_log_t* log;
    MPI_IREDUCE fn=TRACE_TYPE_FN[type];
    int size=0;
    MPI_Type_size(datatype,&size);
    if(log_threshold>0&&log_threshold>count*size) return fn(sendbuf,recvbuf,count,datatype,op,root,comm,request);
    start=timer_fn();
    ret=fn(sendbuf,recvbuf,count,datatype,op,root,comm,request);
    end=timer_fn();
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,0);
    log->comm=comm;
    log->peer=root;
    if(root==tracer_rank){
        log->rcount=count;
        log->rdatatype_size=size;
        log->scount=0;
        log->sdatatype_size=0;
    }else{
        log->scount=count;
        log->sdatatype_size=size;
        log->rcount=0;
        log->rdatatype_size=0;
    }
    cache_request(request,trace_index);
    trace_index++;
    MT_UNLOCK()
    return ret;
}

int MPI_Allreduce(const void *sendbuf, void *recvbuf, int count,
                  MPI_Datatype datatype, MPI_Op op, MPI_Comm comm){
    int ret;
    int type=type_allreduce;
    int idx=-1;
    double start,end;
    trace_log_t* log;
    int size=0;
    MPI_ALLREDUCE fn=TRACE_TYPE_FN[type];
    MPI_Type_size(datatype,&size);
    if(log_threshold>0&&log_threshold>count*size) return fn(sendbuf,recvbuf,count,datatype,op,comm);
    start=timer_fn();
    ret=fn(sendbuf,recvbuf,count,datatype,op,comm);
    end=timer_fn();
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,end);
    log->comm=comm;
    log->rcount=count;
    log->scount=count;
    log->rdatatype_size=size;
    log->sdatatype_size=size;
    trace_index++;
    MT_UNLOCK()
    return ret;
}

int MPI_Iallreduce(const void *sendbuf, void *recvbuf, int count,
                   MPI_Datatype datatype, MPI_Op op, MPI_Comm comm,
                   MPI_Request *request){
    int ret;
    int type=type_iallreduce;
    int idx=-1;
    double start,end;
    int size=0;
    trace_log_t* log;
    MPI_IALLREDUCE fn=TRACE_TYPE_FN[type];
    MPI_Type_size(datatype,&size);
    if(log_threshold>0&&log_threshold>count*size) return fn(sendbuf,recvbuf,count,datatype,op,comm,request);
    start=timer_fn();
    ret=fn(sendbuf,recvbuf,count,datatype,op,comm,request);
    end=timer_fn();
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,0);
    log->comm=comm;
    log->rcount=count;
    log->scount=count;
    log->rdatatype_size=size;
    log->sdatatype_size=size;
    cache_request(request,trace_index);
    trace_index++;
    MT_UNLOCK()
    return ret;
}

int MPI_Barrier(MPI_Comm comm){
    int ret;
    int type=type_barrier;
    int idx=-1;
    double start,end;
    trace_log_t* log;
    MPI_BARRIER fn=TRACE_TYPE_FN[type];
    if(log_threshold>0) return fn(comm);
    start=timer_fn();
    ret=fn(comm);
    end=timer_fn();
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,end);
    log->comm=comm;
    trace_index++;
    MT_UNLOCK()
    return ret;
}

int MPI_Ibarrier(MPI_Comm comm, MPI_Request *request){
    int ret;
    int type=type_ibarrier;
    int idx=-1;
    double start,end;
    trace_log_t* log;
    MPI_IBARRIER fn=TRACE_TYPE_FN[type];
    if(log_threshold>0) return fn(comm,request);
    start=timer_fn();
    ret=fn(comm,request);
    end=timer_fn();
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,0);
    log->comm=comm;
    cache_request(request,trace_index);
    trace_index++;
    MT_UNLOCK()
    return ret;
}
