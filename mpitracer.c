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
#include <dlfcn.h>


#define MT

#ifdef MT
#define MT_LOCK() {if(mt_flag) { pthread_mutex_lock(&log_mutex);}}
#define MT_UNLOCK() {if(mt_flag) { pthread_mutex_unlock(&log_mutex);}}
#else
#define MT_LOCK() {}
#define MT_UNLOCK() {}
#endif


#define END_TS_DELAY 0

#define DEFAULT_BUF_SIZE 1024


static struct list_head request_pool=LIST_HEAD_INIT(request_pool);
static struct htable_node htable[MAX_HASH];


typedef int(*MPI_INIT)(int *argc, char ***argv);
typedef int(*MPI_INIT_THREAD)(int *argc, char ***argv,
    int required, int *provided);
typedef int(*MPI_FINALIZE)();
typedef int(*MPI_SEND)(const void *buf, int count, MPI_Datatype datatype, int dest,
    int tag, MPI_Comm comm);
typedef int(*MPI_ISEND)(const void *buf, int count, MPI_Datatype datatype, int dest,
    int tag, MPI_Comm comm, MPI_Request *request);
typedef int(*MPI_SSEND)(const void *buf, int count, MPI_Datatype datatype, int dest,
    int tag, MPI_Comm comm);
typedef int(*MPI_ISSEND)(const void *buf, int count, MPI_Datatype datatype, int dest,
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
    type_ssend,
    type_issend,
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
    "MPI_Ssend",
    "MPI_Issend",
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

static int TRACE_IGNORE_LIST[MAX_TRACE_FN]={0};
static int ignore_count=0;

static void* TRACE_TYPE_FN[MAX_TRACE_FN];

typedef struct trace_log_struct{
   long id;
   int type;
   long scount;
   long sdatatype_size;
   long rcount;
   long rdatatype_size;
   int peer;
   int tag;
   double start_ts;
   double return_ts;
   double end_ts;
   void* comm;
   void* private;
} trace_log_t;

typedef struct pair_log_struct{
   int src;
   int dst;
   long count;
   long total_msg_size;
   int max_msg_size;
   int min_msg_size;
   float max_bw;
   float min_bw;
   float total_bw;
   double start_ts;
   double end_ts;
} pair_log_t;


static pthread_t tid;
pthread_mutex_t log_mutex;
static int tracer_init_flag=0;
static int tracer_exit_flag=0;
static int writer_exit_flag=0;
static int mt_flag=0;
int issue_found_flag=0;
int tracer_rank=-1;
static int max_trace_num=MAX_TRACE_NUM;
static trace_log_t* probe_log=NULL;
static long trace_index=0;
static double rank_start_ts=0;
static double GHZ=-1;
static int writer_enable=1;
static int trace_reducer_enable=1;
int MPI_program_warning_enable=1;
static int log_threshold=0;
static int total_ranks=0;
pair_log_t* send_pairs=NULL;
pair_log_t* recv_pairs=NULL;
pair_log_t* collective_pairs=NULL;
char root_hostname[256];
char reducer_log_file[256];

time_t app_start_time;

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
    
static char tracer_hostname[256];

static Timer timer_fn=gettimeofday_timer;

static inline void print_log(FILE* fp,trace_log_t* log);


void Ignore_all_fn(){
    int i;
    for(i=0;i<type_COUNT;i++){
        TRACE_IGNORE_LIST[i]=1;
    }
}

void Trace_all_fn(){
    int i;
    for(i=0;i<type_COUNT;i++){
        TRACE_IGNORE_LIST[i]=0;
    }
}

static MPI_Comm pMPI_comm_world=NULL;
static MPI_Datatype myInt=NULL;
static MPI_Datatype myChar=NULL;



void display_info(){
    int i,j;
    printf("MPITRACER\tInject MPI to Trace Traffic\n");
    printf("MPITRACER\tTotal ranks: %d\n",total_ranks);
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
    if(trace_reducer_enable){
        printf("MPITRACER\tThe summary of all trace will be saved to host of rank 0 in path /var/log/mpi_trace_task_<time>.log\n");
    }else{
        printf("MPITRACER\tDo not produce summary of all trace\n");
    }
    if(MPI_program_warning_enable){
        printf("MPITRACER\tWARNING Enabled if find program bugs in using Non-blocking API\n");
    }else{
        printf("MPITRACER\tWARNING Disabled if find program bugs in using Non-blocking API\n");
    }
    if(ignore_count>0){
        j=ignore_count;
        printf("MPITRACER\tFunction Ignored: ");
        for(i=0;i<type_COUNT;i++){
            if(TRACE_IGNORE_LIST[i]==1){
                printf("%s",TRACE_TYPE_NAME[i]);
                j--;
                if(j>0){
                    printf(",");
                }else{
                    printf("\n");
                }
            }
        }
    }
}

#define MAX_WAIT_DELAY 1000
void* log_writer_thread(){
    char log_file[256];
    FILE* fp;
    int writer_index=0;
    int offset=0;
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
            if(!tracer_exit_flag){
                /* writer may be too fast*/
                if(wait_n < MAX_WAIT_DELAY){
                    usleep(1000);
                    wait_n++;
                    continue;
                }else{
                    //printf("why?\n");
                    if(MPI_program_warning_enable) {
                        printf("MPITRACER:\tFound program didn't call MPI_Test/MPI_Wait after use non-blocking API, set MPITRACER_FOUND_ASYNC_BUG_WARNING=0 to disable this Warning\n");
                        issue_found_flag=1;
                    }
                }
            }
        }
        wait_n=0;
        print_log(fp,&probe_log[writer_index]);
        writer_index=(writer_index+1)%max_trace_num;
    }
    fclose(fp);
    return NULL;
}

void init_mpitracer(){
    int i,j;
    char* env;
    char ignores[10][64];
    int count=0;
    char* tmp;
    if(tracer_init_flag) return;
    time(&app_start_time); 
    init_request_pool(&request_pool,DEFAULT_BUF_SIZE);
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
    
    env=getenv("MPITRACER_DISABLE_REDUCER");
    if(env){
        if(atoi(env)==1){
            trace_reducer_enable=0;
        }
    }
    
    env=getenv("MPITRACER_FOUND_ASYNC_BUG_WARNING");
    if(env){
        if(atoi(env)==0){
            MPI_program_warning_enable=0;
        }
    }
    
    env=getenv("MPITRACER_IGNORE");
    if(env){
        count = __split(ignores, env, ",");
        for ( i = 0; i < count; i++){
            tmp=ignores[i];
            for(j=0;j<type_COUNT;j++){
                if(strcmp(tmp,TRACE_TYPE_NAME[j])==0){
                    if(TRACE_IGNORE_LIST[j]!=1){
                        ignore_count++;
                    }
                    TRACE_IGNORE_LIST[j]=1;
                    
                }
            }
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


void init_tracer_statistician(){
    int i;
    recv_pairs=(pair_log_t*)malloc(sizeof(pair_log_t)*total_ranks);
    send_pairs=(pair_log_t*)malloc(sizeof(pair_log_t)*total_ranks);
    collective_pairs=(pair_log_t*)malloc(sizeof(pair_log_t)*DEFAULT_BUF_SIZE);
    for(i=0;i<total_ranks;i++){
        recv_pairs[i].src=i;
        recv_pairs[i].dst=tracer_rank;
        recv_pairs[i].count=0;
        recv_pairs[i].total_msg_size=0;
        recv_pairs[i].max_bw=0;
        recv_pairs[i].min_bw=999;
        recv_pairs[i].max_msg_size=0;
        recv_pairs[i].min_msg_size=99999999;
        send_pairs[i].dst=i;
        send_pairs[i].src=tracer_rank;
        send_pairs[i].count=0;
        send_pairs[i].total_msg_size=0;
        send_pairs[i].max_bw=0;
        send_pairs[i].min_bw=999;
        send_pairs[i].max_msg_size=0;
        send_pairs[i].min_msg_size=99999999;
    }
}

void free_tracer_statistician(){
    free(recv_pairs);
    free(send_pairs);
    free(collective_pairs);
}


void attach_tracer(){
    int i;
    void* ptr = dlsym(RTLD_LOCAL,"ompi_mpi_comm_world");
    if(ptr){
        pMPI_comm_world = (MPI_Comm)ptr;
        myChar = dlsym(RTLD_LOCAL, "ompi_mpi_char");
        myInt = dlsym(RTLD_LOCAL, "ompi_mpi_int");
    }else if(dlsym(RTLD_LOCAL,"iPMI_Init")){
        pMPI_comm_world = (MPI_Comm)0x44000000;
        myChar = (MPI_Datatype)0x4c000101;
        myInt = (MPI_Datatype)0x4c000405;
    }
    MPI_Comm_size(pMPI_comm_world,&total_ranks);
    if(tracer_rank==-1){
        MPI_Comm_rank(pMPI_comm_world,&tracer_rank);
        MPI_Get_processor_name(tracer_hostname,&i);
        tracer_hostname[i]='\0';
        #ifdef DEBUG
        printf("%s\n",tracer_hostname);
        #endif
        if(0==tracer_rank){
            display_info();
        }
    }
}

int MPI_Init(int *argc, char ***argv){
    static MPI_INIT old_fn= NULL;
    int ret=0;
    handle = dlopen("libmpi.so", RTLD_LAZY);
    old_fn= (MPI_INIT)dlsym(handle, "MPI_Init");
    init_mpitracer();
    ret=old_fn(argc, argv);
    attach_tracer();
    init_tracer_statistician();
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
    attach_tracer();
    init_tracer_statistician();
    return ret;
}

void update_pair_data(pair_log_t* pair,int len,double gbps,double start_ts,double end_ts){
    if(pair->count==0) pair->start_ts=start_ts-rank_start_ts;
    pair->count++;
    pair->total_msg_size+=len;
    pair->total_bw+=gbps;
    if(len>pair->max_msg_size) pair->max_msg_size=len;
    if(len<pair->min_msg_size) pair->min_msg_size=len;
    if(gbps>pair->max_bw) pair->max_bw=gbps;
    if(gbps<pair->min_bw) pair->min_bw=gbps;
    pair->end_ts=end_ts-rank_start_ts;
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
    int pair_type=0;
    pair_log_t* pair=NULL;
    switch(log->type){
        case type_send:
        case type_isend:
        case type_ssend:
        case type_issend:
            src=tracer_rank;
            dst=log->peer;
            tag=log->tag;
            rcount=0;
            rsize=0;
            /*log pair only during recv*/
            pair=&send_pairs[dst];
            pair_type=0;
            break;
        case type_recv:
        case type_irecv:
            dst=tracer_rank;
            src=log->peer;
            tag=log->tag;
            scount=0;
            ssize=0;
            /*log pair only during recv*/
            if(src>=0) {
                pair=&recv_pairs[src];
                pair_type=1;
            }
            break;
        case type_bcast:
        case type_ibcast:
            src=log->peer;
            if(src==tracer_rank) {
                /*
                pair=&send_pairs[src];
                pair_type=0;
                */
            }else{
                pair=&recv_pairs[src];
                pair_type=1;
            }

            break;
        case type_reduce:
        case type_ireduce:
            dst=log->peer;
            if(dst!=tracer_rank) {
                pair=&send_pairs[dst];
                pair_type=0;
            }else{
                /*pair=&recv_pairs[src];
                pair_type=1;
                */
            }
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
    if(pair){
        if(pair_type==0) update_pair_data(pair,slen,sgbps,log->start_ts,log->end_ts); 
        if(pair_type==1) update_pair_data(pair,rlen,rgbps,log->start_ts,log->end_ts); 
    }
    fprintf( fp, "%9ld %25s %11.6lf %9.6lf %10.6lf %10p %7d %7d %7d %9d %8d %8d %7.3lf %9d %8d %8d %7.3lf %s\n", log->id,TRACE_TYPE_NAME[log->type],(log->start_ts-rank_start_ts) ,(log->return_ts-log->start_ts),elapse,comm,tag,src,dst,scount,ssize,slen,sgbps,rcount,rsize,rlen,rgbps,debug_info);
}


void record_statistic(pair_log_t* pairs,int count,char* table){
    int i;
    FILE* fp=NULL;
    double elapse=0.0;
    int avg_msg_size;
    double avg_bw;
    double bw_mean;
    char starttime[16];
    struct tm *tm_start;
    int len=0;
    pair_log_t* pair=NULL;
    tm_start = localtime(&app_start_time);
    strftime(starttime,16,"%m%d%H%M%S",tm_start);
    sprintf(reducer_log_file,"/var/log/mpi_trace_task_%s.log",starttime);
    MPI_Get_processor_name(root_hostname,&len);
    fp=fopen(reducer_log_file,"w");
    fprintf(fp,"%16s\t%7s\t%16s\t%7s\t%11s\t%11s\t%16s\t%16s\t%8s\t%8s\t%8s\t%7s\t%7s\t%7s\t%7s\n","SHost","SRC","DHost","DST","Start","Elapse","TotalCount","TotalBytes","Max_msg","Min_msg","Avg_msg","Max_bw","Min_bw","Avg_bw","Bw_mean");
    for(i=0;i<count;i++){
        pair=&pairs[i];
        elapse=pair->end_ts-pair->start_ts;
        avg_msg_size=(int)(pair->total_msg_size/pair->count);
        avg_bw=pair->total_msg_size/(elapse+0.000001)*8*1e-9;
        bw_mean=pair->total_bw/pair->count;
        fprintf(fp,"%16s\t%7d\t%16s\t%7d\t%11.6lf\t%11.6lf\t%16ld\t%16ld\t%8d\t%8d\t%8d\t%7.3lf\t%7.3lf\t%7.3lf\t%7.3lf\n",&table[256*pair->src],pair->src,&table[256*pair->dst],pair->dst,pair->start_ts,elapse,pair->count,pair->total_msg_size, pair->max_msg_size,pair->min_msg_size,avg_msg_size,pair->max_bw,pair->min_bw,avg_bw,bw_mean);
    }
    fclose(fp);
}


void trace_reducer_process(){
    /*for pt2pt only*/
    int i,j;
    int count=0;
    int* len_list=NULL;
    int* displs=NULL;
    int buflens=0;
    int root=0;
    char* hostname_table;
    pair_log_t* local_pairs=NULL;
    pair_log_t* pair_logs=NULL;
    for(i=0;i<total_ranks;i++){
        if(send_pairs[i].count==0) continue;
        count++;
    }
    for(i=0;i<total_ranks;i++){
        if(recv_pairs[i].count==0) continue;
        count++;
    }
    if(count>0){
        local_pairs=(pair_log_t*)malloc(sizeof(pair_log_t)*count);
        j=0;
        for(i=0;i<total_ranks;i++){
            if(send_pairs[i].count==0) continue;
            memcpy(&local_pairs[j],&send_pairs[i],sizeof(pair_log_t));
            j++;     
        }
        for(i=0;i<total_ranks;i++){
            if(recv_pairs[i].count==0) continue;
            memcpy(&local_pairs[j],&recv_pairs[i],sizeof(pair_log_t));
            j++;     
        }
    }
    len_list=(int*)malloc(sizeof(int)*total_ranks);
    MPI_Barrier(pMPI_comm_world);
    MPI_Gather(&count, 1, myInt, len_list, 1, myInt, root, pMPI_comm_world);
    MPI_Barrier(pMPI_comm_world);
    hostname_table=(char*)malloc(256*total_ranks);
    
    if(tracer_rank==root){
        displs=(int*)malloc(sizeof(int)*total_ranks);
        for(i=0;i<total_ranks;i++){
            displs[i]=buflens*sizeof(pair_log_t);
            buflens+=len_list[i];
            len_list[i]=sizeof(pair_log_t)*len_list[i];
        } 
        pair_logs=(pair_log_t*)malloc(sizeof(pair_log_t)*buflens);
    }
    MPI_Gather(tracer_hostname, 256, myChar, hostname_table, 256, myChar, root, pMPI_comm_world);
    MPI_Barrier(pMPI_comm_world);
    MPI_Gatherv(local_pairs, sizeof(pair_log_t)*count, myChar, pair_logs, len_list, displs,myChar, root, pMPI_comm_world);
    MPI_Barrier(pMPI_comm_world);
    free(local_pairs);
    if(tracer_rank==root){
        record_statistic(pair_logs,buflens,hostname_table);
        free(pair_logs);
        free(displs);
    }
    free(hostname_table);
}



int MPI_Finalize(){
    static MPI_FINALIZE old_fn= NULL;
    int ret=0;
    char log_file[256];
    FILE* fp;
    int i,offset;
    Ignore_all_fn();
    MPI_Barrier(pMPI_comm_world);
    tracer_exit_flag=1;
    if(writer_enable){
        writer_exit_flag=1;
        pthread_join(tid,NULL);
    }else{
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
    if(trace_reducer_enable){
        trace_reducer_process();
    }
    #ifdef DEBUG
    view_htable(htable);
    #endif
    free(probe_log);
    free_tracer_statistician();
    if(0==tracer_rank){
        printf("MPITRACER\tPlease check files of %s/%s_<rank_id>.log on each node\n",log_dir,log_prefix);
        printf("MPITRACER\tRecord %ld traces of rank 0\n",(trace_index+1));
        if(issue_found_flag){
            printf("MPITRACER\tProgram issues found that using Non-blocking, check the traces where elapse time is 999.0\n");
        }
        if(trace_reducer_enable){
            printf("MPITRACER\tTrace summary saved to %s:%s\n",root_hostname,reducer_log_file);
        }
    }
    old_fn= (MPI_FINALIZE)dlsym(handle, "MPI_Finalize");
    ret=old_fn();
    dlclose(handle);
    return ret;
}


inline static void new_log(trace_log_t* log,int type,long idx,double start_ts,double return_ts,double end_ts){
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
    if((TRACE_IGNORE_LIST[type])||(log_threshold>0&&log_threshold>count*size)) return fn(buf,count,datatype,dest,tag,comm);
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
    if((TRACE_IGNORE_LIST[type])||(log_threshold>0&&log_threshold>count*size)) return fn(buf,count,datatype,dest,tag,comm,request);
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

int MPI_Ssend(const void *buf, int count, MPI_Datatype datatype, int dest,
    int tag, MPI_Comm comm){
    int ret;
    int type=type_ssend;
    double start,end;
    int idx=-1;
    trace_log_t* log;
    MPI_SSEND fn=TRACE_TYPE_FN[type];
    int size=0;
    MPI_Type_size(datatype,&size);
    if((TRACE_IGNORE_LIST[type])||(log_threshold>0&&log_threshold>count*size)) return fn(buf,count,datatype,dest,tag,comm);
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

int MPI_Issend(const void *buf, int count, MPI_Datatype datatype, int dest,
    int tag, MPI_Comm comm, MPI_Request *request){
    int ret;
    int type=type_issend;
    double start,end;
    int idx=-1;
    trace_log_t* log;
    MPI_ISSEND fn=TRACE_TYPE_FN[type];
    int size=0;
    MPI_Type_size(datatype,&size);
    if((TRACE_IGNORE_LIST[type])||(log_threshold>0&&log_threshold>count*size)) return fn(buf,count,datatype,dest,tag,comm,request);
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
    int tmp_count=0;
    MPI_Type_size(datatype,&size);
    if((TRACE_IGNORE_LIST[type])||(log_threshold>0&&log_threshold>count*size)) return fn(buf,count,datatype,source,tag,comm,status);
    start=timer_fn();
    ret=fn(buf,count,datatype,source,tag,comm,status);
    end=timer_fn();
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,end);
    if(status!=MPI_STATUS_IGNORE){
        log->peer=status->MPI_SOURCE;
        log->tag=status->MPI_TAG;
        MPI_Get_count(status,datatype,&tmp_count);
        log->rcount=tmp_count;
    }else{
        log->rcount=count;
        log->peer=source;
        log->tag=tag;
    }
    log->comm=comm;
    log->rdatatype_size=size;
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
    if((TRACE_IGNORE_LIST[type])||(log_threshold>0&&log_threshold>count*size)) return fn(buf,count,datatype,source,tag,comm,request);
    start=timer_fn();
    ret=fn(buf,count,datatype,source,tag,comm,request);
    end=timer_fn();
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,0);
    log->rcount=count;
    log->peer=source;
    log->tag=tag;
    log->comm=comm;
    log->rdatatype_size=size;
    log->private=datatype;
    cache_request(request,trace_index);
    trace_index++;
    MT_UNLOCK()
    return ret;
}

inline static void update_end_ts_of_cached_logs(int count,MPI_Request* array_of_requests,MPI_Status* array_of_statuses,double end){
    int i;
    void* ptr;
    struct request_node* r;
    int idx;
    MPI_Status* status;
    int tmp_count;
    trace_log_t* log; 
    #ifdef DEBUG
    if(tracer_rank==0) {
        printf("check request %p %d\t",array_of_requests,count);
        for(i=0;i<count;i++){
            printf("%p\t",array_of_requests+i);
        }
        printf("\n");
    }
    #endif
    for(i=0;i<count;i++){
       ptr=(void*)((MPI_Request*)array_of_requests+i); 
       r=hash_find_request(ptr,htable);
       if(!r){
          #ifdef DEBUG
          if(tracer_rank==0) printf("request not found? %p of %d\n",ptr,i);
          #endif
          continue;
       }else{
          idx=r->index%max_trace_num;
          log=&probe_log[idx];
          status=((MPI_Status*)array_of_statuses+i);
          if(status!=MPI_STATUS_IGNORE){
              if(log->type==type_irecv){
                  log->peer=status->MPI_SOURCE;
                  log->tag=status->MPI_TAG;
                  MPI_Get_count(status,log->private,&tmp_count);
                  log->rcount=tmp_count;
              }
          }
          log->end_ts=end;
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
    MT_LOCK()
    update_end_ts_of_cached_logs(1,request,status,end);
    if((TRACE_IGNORE_LIST[type])||(log_threshold>0)){
        MT_UNLOCK()
        return ret;
    }
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
    MT_LOCK()
    update_end_ts_of_cached_logs(count,array_of_requests,array_of_statuses,end);
    if((TRACE_IGNORE_LIST[type])||(log_threshold>0)) {
        MT_UNLOCK()
        return ret;
    }
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
    MT_LOCK()
    update_end_ts_of_cached_logs(1,request,status,end);
    if((TRACE_IGNORE_LIST[type])||(log_threshold>0)) {
        MT_UNLOCK()
        return ret;
    }
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
    MT_LOCK()
    update_end_ts_of_cached_logs(count,array_of_requests,array_of_statuses,end);
    if((TRACE_IGNORE_LIST[type])||(log_threshold>0)) {
        MT_UNLOCK()
        return ret;
    }
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
    if((TRACE_IGNORE_LIST[type])||(log_threshold>0&&(log_threshold>recvcount*rsize||log_threshold>sendcount*ssize))) return ret=fn(sendbuf,sendcount,sendtype,recvbuf,recvcount,recvtype,comm);
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
    if((TRACE_IGNORE_LIST[type])||(log_threshold>0&&(log_threshold>recvcount*rsize||log_threshold>sendcount*ssize))) return ret=fn(sendbuf,sendcount,sendtype,recvbuf,recvcount,recvtype,comm,request);
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
    if((TRACE_IGNORE_LIST[type])||(log_threshold>0&&log_threshold>count*size)) return fn(buffer,count,datatype,root,comm);
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
    if((TRACE_IGNORE_LIST[type])||(log_threshold>0&&log_threshold>count*size)) return fn(buffer,count,datatype,root,comm,request);
    start=timer_fn();
    ret=fn(buffer,count,datatype,root,comm,request);
    end=timer_fn();
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,0);
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
    if((TRACE_IGNORE_LIST[type])||(log_threshold>0&&log_threshold>count*size)) return fn(sendbuf,recvbuf,count,datatype,op,root,comm);
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
    if((TRACE_IGNORE_LIST[type])||(log_threshold>0&&log_threshold>count*size)) return fn(sendbuf,recvbuf,count,datatype,op,root,comm,request);
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
    if((TRACE_IGNORE_LIST[type])||(log_threshold>0&&log_threshold>count*size)) return fn(sendbuf,recvbuf,count,datatype,op,comm);
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
    if((TRACE_IGNORE_LIST[type])||(log_threshold>0&&log_threshold>count*size)) return fn(sendbuf,recvbuf,count,datatype,op,comm,request);
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
    if((TRACE_IGNORE_LIST[type])||(log_threshold>0)) return fn(comm);
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
    if((TRACE_IGNORE_LIST[type])||(log_threshold>0)) return fn(comm,request);
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
