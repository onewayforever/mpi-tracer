#ifndef __MPI_TRACER_H
#define __MPI_TRACER_H
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
#define DEFAULT_GROUP_SIZE 4096

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
typedef int (*MPI_BSEND)(const void *buf, int count, MPI_Datatype datatype,
    int dest, int tag, MPI_Comm comm);
typedef int(*MPI_IBSEND)(const void *buf, int count, MPI_Datatype datatype, int dest,
    int tag, MPI_Comm comm, MPI_Request *request);
typedef int (*MPI_RSEND)(const void *buf, int count, MPI_Datatype datatype,
    int dest, int tag, MPI_Comm comm);
typedef int(*MPI_IRSEND)(const void *buf, int count, MPI_Datatype datatype, int dest,
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

typedef int (*MPI_COMM_SPLIT)(MPI_Comm comm, int color, int key,
    MPI_Comm *newcomm);

typedef int (*MPI_COMM_DUP)(MPI_Comm comm, MPI_Comm *newcomm);

typedef int (*MPI_COMM_CREATE)(MPI_Comm comm, MPI_Group group, MPI_Comm *newcomm);

typedef int(*MPI_GATHER)(const void *sendbuf, int sendcount,
    MPI_Datatype sendtype, void *recvbuf, int recvcount, 
    MPI_Datatype recvtype, int root, MPI_Comm comm);
typedef int(*MPI_IGATHER)(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                void *recvbuf, int recvcount, MPI_Datatype recvtype,
                int root, MPI_Comm comm, MPI_Request *request);
typedef int(*MPI_ALLGATHER)(const void *sendbuf, int sendcount, 
    MPI_Datatype sendtype, void *recvbuf, int recvcount, 
    MPI_Datatype recvtype, MPI_Comm comm);
typedef int(*MPI_IALLGATHER)(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                   void *recvbuf, int recvcount, MPI_Datatype recvtype,
                   MPI_Comm comm,  MPI_Request *request);
typedef int(*MPI_SCATTER)(const void *sendbuf, int sendcount, 
    MPI_Datatype sendtype, void *recvbuf, int recvcount, 
    MPI_Datatype recvtype, int root, MPI_Comm comm);
/* must keep same order with TRACE_TYPE_NAME */
enum TRACE_TYPE{
    type_send,
    type_recv,
    type_isend,
    type_irecv,
    type_ssend,
    type_issend,
    type_bsend,
    type_ibsend,
    type_rsend,
    type_irsend,
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
    type_comm_split,
    type_comm_dup,
    type_comm_create,
    type_gather,
    type_igather,
    type_allgather,
    type_iallgather,
    type_scatter,
    type_COUNT
}; 
#define MAX_TRACE_NUM 100000
#define MAX_TRACE_FN 100

typedef struct trace_log_struct{
    long id;
    int type;
    long scount;
    long sdatatype_size;
    long rcount;
    long rdatatype_size;
    int peer;
    int gpeer;
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

typedef struct request_node{
    long key;
    int index;
    struct list_head node;
} request_node_t;

typedef struct group_node{
    long key;
    int index;
    int num;
    int* map;
    struct list_head node;
} group_node_t;

#endif
