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
#include "mpitracer.h"


static struct list_head request_pool=LIST_HEAD_INIT(request_pool);
static struct list_head group_pool=LIST_HEAD_INIT(group_pool);
static struct htable_node request_htable[MAX_HASH];
static struct htable_node group_htable[MAX_HASH];


/* must keep same order with TRACE_TYPE */
static char TRACE_TYPE_NAME[MAX_TRACE_FN][25]={
    "MPI_Send",
    "MPI_Recv",
    "MPI_Isend",
    "MPI_Irecv",
    "MPI_Ssend",
    "MPI_Issend",
    "MPI_Bsend",
    "MPI_Ibsend",
    "MPI_Rsend",
    "MPI_Irsend",
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
    "MPI_Comm_split",
    "MPI_Comm_dup",
    "MPI_Comm_create",
};

#define OPENMPI 1
#define INTELMPI 2

static int use_mpi=0;

static int TRACE_IGNORE_LIST[MAX_TRACE_FN]={0};
static int ignore_count=0;

static void* TRACE_TYPE_FN[MAX_TRACE_FN];


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
static int writer_enable=1;
static int trace_reducer_enable=1;
int MPI_program_warning_enable=1;
int log_ts_absolute=0;
static int log_threshold=0;
static int total_ranks=0;
pair_log_t* send_pairs=NULL;
pair_log_t* recv_pairs=NULL;
pair_log_t* collective_pairs=NULL;
char root_hostname[256];
char reducer_log_file[256];

time_t app_start_time;


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
static MPI_Comm pompi_mpi_comm_null=NULL;


static int inline rank2global(int rank,MPI_Comm comm){
    group_node_t* g;
    if(comm==pMPI_comm_world)
        return rank;
    g=hash_find_obj((long)comm,group_htable,group_node_t);
    if(!g) return -1;
    if((rank>=g->num)||rank<0){
        printf("unkown group mapping %p total:%d rank:%d\n",comm,g->num,rank);
        return -1;
    }else{
        return g->map[rank];
    }
}

inline int check_group_conflict(group_node_t* item,group_node_t* add){
    printf("find group conflict!\n");
    return 1; 
}

void update_group(MPI_Comm comm){
    group_node_t* g;
    MPI_Group my_group,world_group;
    int size=0;
    int* map=NULL;
    int* tmp=NULL;
    int i;
    if(comm==pompi_mpi_comm_null)
        return;
    g=hash_find_obj((long)comm,group_htable,group_node_t);
    if(g){
    }else{
        g=pool_pop(&group_pool,group_node_t);
        if(g){
            g->key=(long)comm;
            g->num=0;
            g->map=NULL;
            hash_add_obj(g,group_htable,group_node_t,check_group_conflict);
        }
    }    
    if(g->map) free(g->map);
    MPI_Comm_size(comm,&size);
    if(size>0){
        MPI_Comm_group(comm,&my_group);
        MPI_Comm_group(pMPI_comm_world,&world_group);
        tmp=(int*)malloc(sizeof(int)*size);
        for(i=0;i<size;i++) tmp[i]=i;
        map=(int*)malloc(sizeof(int)*size);
        MPI_Group_translate_ranks(my_group,size,tmp,world_group,map);
        g->num=size;
        g->map=map;
        free(tmp);
    }
}

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
        printf("MPITRACER\tThe summary of all trace will be saved to host of rank 0 in path /var/tmp/mpi_trace_task_<time>.log\n");
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
    fprintf( fp, "%9s %25s %11s %9s %10s %8s %7s %7s %7s %7s %7s %9s %8s %8s %7s %9s %8s %8s %7s\n","ID","MPI_TYPE","TimeStamp","Call","Elapse","Comm","Tag","SRC","DST","GSRC","GDST","SCount","SBuf_B","SLen_B","SBW_Gbps","RCount","RBuf_B","RLen_B","RBW_Gbps");
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

void init_request(request_node_t* obj){
    obj->key = 0;
    obj->index = -1;
}

void init_group(group_node_t* obj){
    obj->key = 0;
    obj->num = 0;
    obj->map = NULL;
}

void init_mpitracer(){
    int i,j;
    char* env;
    char ignores[10][64];
    int count=0;
    char* tmp;
    if(tracer_init_flag) return;
    time(&app_start_time); 
    init_pool(&request_pool,request_node_t,DEFAULT_BUF_SIZE,init_request);
    init_pool(&group_pool,group_node_t,DEFAULT_GROUP_SIZE,init_group);
    init_htable(request_htable);
    init_htable(group_htable);
    env=getenv("MPITRACER_MPI");
    if(env){
        if(strcmp(env,"openmpi")==0){
            use_mpi=OPENMPI;
        }
        if(strcmp(env,"intelmpi")==0){
            use_mpi=INTELMPI;
        }
    }
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

    env=getenv("MPITRACER_LOG_TS_ABSOLUTE");
    if(env){
        if(atoi(env)==1){
            log_ts_absolute=1;
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
    int detect=use_mpi;
    void* ptr=NULL; 
    if(detect==0){
        ptr = dlsym(RTLD_LOCAL,"ompi_mpi_comm_world");
        if(ptr){
            detect=OPENMPI;
        }else if(dlsym(RTLD_LOCAL,"iPMI_Init")){
            detect=INTELMPI;
        }
    }
    switch(detect){
    case OPENMPI:
        printf("MPITRACER\tUse openmpi!\n");
        pMPI_comm_world = (MPI_Comm)ptr;
        myChar = dlsym(RTLD_LOCAL, "ompi_mpi_char");
        myInt = dlsym(RTLD_LOCAL, "ompi_mpi_int");
        pompi_mpi_comm_null = dlsym(RTLD_LOCAL, "ompi_mpi_comm_null");
        break;
    case INTELMPI:
        printf("MPITRACER\tUse intelmpi!\n");
        pMPI_comm_world = (MPI_Comm)0x44000000;
        myChar = (MPI_Datatype)0x4c000101;
        myInt = (MPI_Datatype)0x4c000405;
        break;
    default:
        printf("MPITRACER\tCan not identify the mpi to inject. Try setting env MPITRACER_MPI=openmpi/intelmpi manually!\n");
        exit(0);
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
    int gsrc=-1;
    int gdst=-1;
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
    double start_ts_print=log->start_ts;
    pair_log_t* pair=NULL;
    switch(log->type){
        case type_send:
        case type_isend:
        case type_ssend:
        case type_issend:
        case type_bsend:
        case type_ibsend:
        case type_rsend:
        case type_irsend:
            src=tracer_rank;
            dst=log->peer;
            gsrc=tracer_rank;
            gdst=log->gpeer;
            tag=log->tag;
            rcount=0;
            rsize=0;
            /*log pair only during recv*/
            pair=&send_pairs[gdst];
            pair_type=0;
            break;
        case type_recv:
        case type_irecv:
            dst=tracer_rank;
            src=log->peer;
            gdst=tracer_rank;
            gsrc=log->gpeer;
            tag=log->tag;
            scount=0;
            ssize=0;
            /*log pair only during recv*/
            if(gsrc>=0) {
                pair=&recv_pairs[gsrc];
                pair_type=1;
            }
            break;
        case type_bcast:
        case type_ibcast:
            src=log->peer;
            gsrc=log->gpeer;
            if(gsrc==tracer_rank) {
                /*
                pair=&send_pairs[src];
                pair_type=0;
                */
            }else{
                pair=&recv_pairs[gsrc];
                pair_type=1;
            }

            break;
        case type_reduce:
        case type_ireduce:
            dst=log->peer;
            gdst=log->gpeer;
            if(gdst!=tracer_rank) {
                pair=&send_pairs[gdst];
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
        case type_comm_split:
        case type_comm_dup:
        case type_comm_create:
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
    if(!log_ts_absolute){
        start_ts_print = log->start_ts - rank_start_ts;
    }
    fprintf( fp, "%9ld %25s %17.6lf %9.6lf %10.6lf %10p %7d %7d %7d %7d %7d %9d %8d %8d %7.3lf %9d %8d %8d %7.3lf %s\n", log->id,TRACE_TYPE_NAME[log->type],start_ts_print,(log->return_ts-log->start_ts),elapse,comm,tag,src,dst,gsrc,gdst,scount,ssize,slen,sgbps,rcount,rsize,rlen,rgbps,debug_info);
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
    sprintf(reducer_log_file,"/var/tmp/mpi_trace_task_%s.log",starttime);
    MPI_Get_processor_name(root_hostname,&len);
    fp=fopen(reducer_log_file,"w");
    fprintf(fp,"%16s\t%7s\t%16s\t%7s\t%11s\t%11s\t%16s\t%16s\t%8s\t%8s\t%8s\t%7s\t%7s\t%7s\t%7s\n","SHost","SRC","DHost","DST","Start","Elapse","TotalCount","TotalBytes","Max_msg","Min_msg","Avg_msg","Max_bw","Min_bw","Avg_bw","Bw_mean");
    for(i=0;i<count;i++){
        pair=&pairs[i];
        elapse=pair->end_ts-pair->start_ts;
        avg_msg_size=(int)(pair->total_msg_size/pair->count);
        avg_bw=pair->total_msg_size/(elapse+0.000001)*8*1e-9;
        bw_mean=pair->total_bw/pair->count;
        //printf("%lf,%ld,%lf\n",pair->total_bw,pair->count,bw_mean);
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
        fprintf( fp, "%9s\t%25s\t%17s\t%9s\t%10s\t%10s\t%7s\t%7s\t%7s\t%7s\t%7s\t%9s\t%8s\t%8s\t%7s\t%9s\t%8s\t%8s\t%7s\n","ID","MPI_TYPE","TimeStamp","Call","Elapse","Comm","Tag","SRC","DST","GSRC","GDST","SCount","SBuf_B","SLen_B","SBW_Gbps","RCount","RBuf_B","RLen_B","RBW_Gbps");
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
    view_htable(request_htable);
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

inline int check_request_conflict(request_node_t* item,request_node_t* add){
    if(MPI_program_warning_enable) {
        printf("MPITRACER:\tFound the program reused the request before call MPI_Test/MPI_Wait, set MPITRACER_FOUND_ASYNC_BUG_WARNING=0 to disable this Warning\n");
        issue_found_flag=1;
    }
    return 1; 
}

inline static void cache_request(MPI_Request* request,int index){
    struct request_node* r;
    r=pool_pop(&request_pool,request_node_t);
    if(r){
        r->key=(long)request;
        r->index=index;
        hash_add_obj(r,request_htable,request_node_t,check_request_conflict);
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
    log->gpeer=rank2global(log->peer,comm);
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
    log->gpeer=rank2global(log->peer,comm);
    log->scount=count;
    log->sdatatype_size=size;
    log->tag=tag;
    cache_request(request,trace_index);
    trace_index++;
    MT_UNLOCK()
    return ret;
}

int MPI_Bsend(const void *buf, int count, MPI_Datatype datatype, int dest,
    int tag, MPI_Comm comm){
    int ret;
    int type=type_bsend;
    double start,end;
    int idx=-1;
    trace_log_t* log;
    MPI_BSEND fn=TRACE_TYPE_FN[type];
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
    log->gpeer=rank2global(log->peer,comm);
    log->scount=count;
    log->sdatatype_size=size;
    log->tag=tag;
    trace_index++;
    MT_UNLOCK()
    return ret;
}

int MPI_Ibsend(const void *buf, int count, MPI_Datatype datatype, int dest,
    int tag, MPI_Comm comm, MPI_Request *request){
    int ret;
    int type=type_ibsend;
    double start,end;
    int idx=-1;
    trace_log_t* log;
    MPI_IBSEND fn=TRACE_TYPE_FN[type];
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
    log->gpeer=rank2global(log->peer,comm);
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
    log->gpeer=rank2global(log->peer,comm);
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
    log->gpeer=rank2global(log->peer,comm);
    log->scount=count;
    log->sdatatype_size=size;
    log->tag=tag;
    cache_request(request,trace_index);
    trace_index++;
    MT_UNLOCK()
    return ret;
}

int MPI_Rsend(const void *buf, int count, MPI_Datatype datatype, int dest,
    int tag, MPI_Comm comm){
    int ret;
    int type=type_rsend;
    double start,end;
    int idx=-1;
    trace_log_t* log;
    MPI_RSEND fn=TRACE_TYPE_FN[type];
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
    log->gpeer=rank2global(log->peer,comm);
    log->scount=count;
    log->sdatatype_size=size;
    log->tag=tag;
    trace_index++;
    MT_UNLOCK()
    return ret;
}

int MPI_Irsend(const void *buf, int count, MPI_Datatype datatype, int dest,
    int tag, MPI_Comm comm, MPI_Request *request){
    int ret;
    int type=type_irsend;
    double start,end;
    int idx=-1;
    trace_log_t* log;
    MPI_IRSEND fn=TRACE_TYPE_FN[type];
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
    log->gpeer=rank2global(log->peer,comm);
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
        log->gpeer=rank2global(log->peer,comm);
        log->tag=status->MPI_TAG;
        MPI_Get_count(status,datatype,&tmp_count);
        log->rcount=tmp_count;
    }else{
        log->rcount=count;
        log->peer=source;
        log->gpeer=rank2global(log->peer,comm);
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
    log->gpeer=rank2global(log->peer,comm);
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
        r=hash_find_obj((long)ptr,request_htable,request_node_t);
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
                    log->gpeer=rank2global(log->peer,log->comm);
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
    log->gpeer=rank2global(log->peer,comm);
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
    log->gpeer=rank2global(log->peer,comm);
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
    log->gpeer=rank2global(log->peer,comm);
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
    log->gpeer=rank2global(log->peer,comm);
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

int MPI_Comm_split(MPI_Comm comm, int color, int key,
    MPI_Comm *newcomm){
    MPI_COMM_SPLIT old_fn=NULL;
    int ret=0;
    int type=type_comm_split;
    int idx=-1;
    double start,end;
    trace_log_t* log;
    handle = dlopen("libmpi.so", RTLD_LAZY);
    old_fn= (MPI_COMM_SPLIT)dlsym(handle, "MPI_Comm_split");
    start=timer_fn();
    ret=old_fn(comm,color,key,newcomm);
    end=timer_fn();
    update_group(*newcomm);

    if((TRACE_IGNORE_LIST[type])||(log_threshold>0)) return ret;
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,end);
    log->comm=*newcomm;
    trace_index++;
    MT_UNLOCK()
    return ret;
}

int MPI_Comm_dup(MPI_Comm comm, MPI_Comm *newcomm){
    MPI_COMM_DUP old_fn=NULL;
    int ret=0;
    int type=type_comm_dup;
    int idx=-1;
    double start,end;
    trace_log_t* log;
    handle = dlopen("libmpi.so", RTLD_LAZY);
    old_fn= (MPI_COMM_DUP)dlsym(handle, "MPI_Comm_dup");
    start=timer_fn();
    ret=old_fn(comm,newcomm);
    end=timer_fn();
    update_group(*newcomm);

    if((TRACE_IGNORE_LIST[type])||(log_threshold>0)) return ret;
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,end);
    log->comm=*newcomm;
    trace_index++;
    MT_UNLOCK()
    return ret;
}

int MPI_Comm_create(MPI_Comm comm, MPI_Group group, MPI_Comm *newcomm){
    MPI_COMM_CREATE old_fn=NULL;
    int ret=0;
    int type=type_comm_create;
    int idx=-1;
    double start,end;
    trace_log_t* log;
    handle = dlopen("libmpi.so", RTLD_LAZY);
    old_fn= (MPI_COMM_CREATE)dlsym(handle, "MPI_Comm_create");
    start=timer_fn();
    ret=old_fn(comm,group,newcomm);
    end=timer_fn();
    update_group(*newcomm);

    if((TRACE_IGNORE_LIST[type])||(log_threshold>0)) return ret;
    MT_LOCK()
    idx=trace_index%max_trace_num;
    log=&probe_log[idx];
    new_log(log,type,trace_index,start,end,end);
    log->comm=*newcomm;
    trace_index++;
    MT_UNLOCK()
    return ret;
}
