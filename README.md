# MPI-TRACER
The realtime monitor for tracing the MPI(Message Passing Interface) program behavior, including  MPI activities and communication performance.

NO NEED to recompile the target MPI application, so it can be used for  3rd party application tracing



## What can MPI-TRACER do?

TotalBytes send among hosts 

![mpiGraph_totalbytes](images/mpiGraph_host_totalbytes.png)  

Max bandwidth among ranks

![mpiGraph_maxbw](images/mpiGraph_maxbw.png) 

Mean bandwidth among ranks

![mpiGraph_bwmean](images/mpiGraph_bwmean.png)


##  Install 

step1: $ make 

step2: Copy the mpitracer.so to the same path at all hosts




## Usage
1. Run 

Add mpitracer.so as the LD_PRELOAD library 
Use -x LD_PRELOAD=/path/to/mpitracer.so during mpirun

Example:

mpirun  --hostfile myhosts -np 64 -npernode 32  -x LD_PRELOAD=/path/to/mpitracer.so /path/to/MPIApp

2. Get results 

* Find trace log of each process in /dev/shm/mpi_trace_\<rankid\>.log

* Find summary of the task in /var/tmp/mpi_trace_task_\<start_timestamp\>.log on host of rank 0（Only log pt2pt traffics）

3. Analyze the results

   View the python scripts in scripts/ as a reference

## Parameters 

MPI tracer use environment variables to pass parameters. The available options are listed below.

* MPITRACER_MPI

  Assign which kind of mpi to use, default: auto detect

  Options:

  ​       openmpi

  ​       intelmpi

* MPITRACER_LOG_SIZE

   Log entries per rank, default: 100000

* MPITRACER_TIMER

   The timer used for mearsuring the running time of the methods

   Options:

  ​     GETTIMEOFDAY    use gettimeofday to measure the time 

  ​     TSC                          use TSC to measure the time, CPU Freq should be passed by MPITRACER_TSC_GHZ

* MPITRACER_TSC_GHZ

  Frequence of the CPU to enable TSC timer, eg 2.0 

* MPITRACER_LOG_DIR

  Log path, default: /dev/shm  

* MPITRACER_LOG_PREFIX

  Log file prefix, default: mpi_trace, and the log file will be record as mpi_trace_\<rankid\>.log

* MPITRACER_LOG_TS_ABSOLUTE
   
  The start timestamp of traces are logged in form of the relative time to the start of the rank, you can set MPITRACER_LOG_TS_ABSOLUTE=1 to log the abosulte time to align the timestamp among different ranks 

* MPITRACER_DELAY_WRITER

  MPI-tracer will create a writer thread to write the logs to the file on each rank process, you can set MPITRACER_DELAY_WRITER=1 to disable writer thread and don't  flush the logs to file during the application runtime util MPI_Finalize stage

* MPITRACER_THRESHOLD

  MPI-tracer will log only the MPI behavior with message size larger than MPITRACER_THRESHOLD, default: 0 (log everything)

* MPITRACER_DISABLE_REDUCER

   MPI-tracer will summary all traces caught in each host and save to a single host, set to 0 to disable it, default: 1 (enable)

* MPITRACER_FOUND_ASYNC_BUG_WARNING

   MPI-tracer can be used to find program issues in using  non-blocking functions,  eg. MPI_Request is not used in pairs of MPI_Ixxxx and MPI_Wait/Test, set to 0 to disable it, default: 1 (enable)

* MPITRACER_IGNORE

   MPI-tracer will ignore the designated functions , separate by comma, eg. MPITRACER_IGNORE=MPI_Bcast,MPI_Barrier

Example:

mpirun  --hostfile myhosts -np 64 -npernode 32 -x MPITRACER_TSC_GHZ=2.5 -x MPITRACER_LOG_SIZE=200000 -x MPITRACER_TIMER=GETTIMEOFDAY  -x LD_PRELOAD=/path/to/mpitracer.so /path/to/MPIApp



## Log Format


### Trace Log format
```
       ID                  MPI_TYPE   TimeStamp      Call     Elapse     Comm     Tag     SRC     DST    GSRC    GDST    SCount   SBuf_B   SLen_B SBW_Gbps    RCount   RBuf_B   RLen_B RBW_Gbps
        7                  MPI_Send    0.385385  0.000001   0.000001  0x2ab27b0    9001      15      14      15      14         3        4       12   0.101         0        0        0   0.000
        8                  MPI_Recv    0.385386  0.000321   0.000321  0x2ab27b0    9001      14      15      14      15         0        0        0   0.000         3        4       12   0.000
        9                  MPI_Recv    0.792978  0.089892   0.089892  0x2ab27b0    9001      14      15      14      15         0        0        0   0.000         1        4        4   0.000
       10                  MPI_Recv    0.928106  0.002097   0.002097  0x2a49c00    2001       2      15      14      15         0        0        0   0.000         1  4327432  4327432  16.510
       11                  MPI_Recv    0.930989  0.005134   0.005134  0x2a1fb60    1001       0      15       3      15         0        0        0   0.000    122562        8   980496   1.528
       12                  MPI_Recv    0.938825  0.000056   0.000056  0x2a1fb60    1001       0      15       3      15         0        0        0   0.000     24141        8   193128  27.576
       13                  MPI_Send    0.938882  0.000348   0.000348  0x2a1fb60    1001      15       1      15       7     27855        8   222840   5.121         0        0        0   0.000
       14                 MPI_Irecv    0.939233  0.000000   0.000503  0x2a1fb60    1001       0      15       3      15         0        0        0   0.000    118848        8   950784  15.127
       15                  MPI_Send    0.939234  0.000499   0.000499  0x2a1fb60    1001      15       0      15       3    118848        8   950784  15.243         0        0        0   0.000
       16                  MPI_Wait    0.939735  0.000001   0.000001      (nil)      -1      -1      -1      -1      -1         1        0        0   0.000         0        0        0   0.000
       17                 MPI_Irecv    0.939737  0.000000   0.001087  0x2a1fb60    1001       1      15       7      15         0        0        0   0.000    118848        8   950784   6.998
       18                  MPI_Send    0.939737  0.001087   0.001087  0x2a1fb60    1001      15       1      15       7    118848        8   950784   6.998         0        0        0   0.000
       19                  MPI_Wait    0.940824  0.000000   0.000000      (nil)      -1      -1      -1      -1      -1         1        0        0   0.000         0        0        0   0.000
       20                 MPI_Irecv    0.940824  0.000001   0.000709  0x2a1fb60    1001       0      15       3      15         0        0        0   0.000    118848        8   950784  10.727

```


| Column    | Descrpition                                                  |
| --------- | ------------------------------------------------------------ |
| MPI_TYPE  | the MPI_xxx functions to hook and log, available functions are listed in the next section |
| TimeStamp | seconds since MPI_Init is called by default, or absolute timestamp if MPITRACER_LOG_TS_ABSOLUTE is set to 1 |
| Call      | the running time of the called function                      |
| Elapse    | if the function is synchronous, eg MPI_Send, it is equal to Call<br />if the function is asynchronous, eg MPI_Isend, it is the time between the asychronous function being called and its asynchronous request being checked positive by MPI_Test or MPI_Wait |
| Comm      | the Commnunicator of the function                            |
| Tag       | the tag of the function                                      |
| SRC       | the source rank of the function commnunicator, display -1 if NA            |
| DST       | the destination rank of the function commnuicator, display -1 if NA       |
| GSRC      | the source rank of the world commnuicator, display -1 if NA            |
| GDST      | the destination rank of the world commnuicator, display -1 if NA       |
| SCount    | the count of buffers for sending                             |
| SBuf_B    | the size of a single buffer for sending, in bytes            |
| SLen_B    | the total size of sending buffer, SLen_B = SCount * SCount, in bytes |
| SBW_Gbps  | the bandwidth of sending process SBW_Gbps = SLen_B/Elapse, formatted to Gbps |
| RCount    | the count of buffers for receiving                           |
| RBuf_B    | the size of a single buffer for receiving, in Bytes          |
| RLen_B    | the total size of receiving buffer, RLen_B = RCount * RBuf_B, in bytes |
| RBW_Gbps  | the bandwidth of receiving process RBW_Gbps = RLen_B/Elapse, formatted to Gbps |



### Task Summary Log format



Pt2pt/bcast/reduce traffic are supportted 

```
          SHost            SRC            DHost            DST       Start          Elapse           TotalCount              TotalBytes         Max_msg         Min_msg         Avg_msg         Max_bw  Min_bw  Avg_bw Bw_mean
        ihostn01              1         ihostn00              0    4.930473      518.642593                42804             24046669008        29360128               4          561785         50.433   0.000   0.371  10.701
        ihostn02              2         ihostn00              0    4.930495      518.656857                13080              1367319360        29245440               4          104535         51.540   0.000   0.021   6.502
        ihostn03              3         ihostn00              0   14.120866      490.132643                 2426              1755566080        29360128            2048          723646         41.648   0.003   0.029   9.031
```

| Column     | Descrpition                                                  |
| ---------- | ------------------------------------------------------------ |
| SHost      | the hostname where source rank located at                    |
| SRC        | the source rank                                              |
| DHost      | the hostname where destination rank located at               |
| DST        | the destination rank                                         |
| Start      | the start time(s)                                            |
| Elapse     | the total time of the communication last between SRC and DST |
| TotalCount | the total messages have been sent                            |
| TotalBytes | the total bytes have been sent, in bytes                     |
| Max_msg    | the max size of all messages, in bytes                       |
| Min_msg    | the min size of all messages, in bytes                       |
| Avg_msg    | the average size of all messages, in bytes                   |
| Max_bw     | the max bandwidth of all traffics between SRC and DST, formatted to Gbps |
| Min_bw     | the min bandwidth of all traffics between SRC and DST, formatted to Gbps |
| Avg_bw     | the average bandwidth of all traffics between SRC and DST, Avg_bw=TotalBytes/Elapse, formatted to Gbps |
| Bw_mean    | the average bandwidth of all traffics between SRC and DST, Bw_mean=Sum(bw)/TotalCount, formatted to Gbps |






## MPI Hooks

The available MPI functions to trace now

MPI_Send

MPI_Recv

MPI_Isend

MPI_Irecv

MPI_Ssend

MPI_Issend

MPI_Bsend

MPI_Ibsend

MPI_Rsend

MPI_Irsend

MPI_Wait

MPI_Waitall

MPI_Test

MPI_Testall

MPI_Bcast

MPI_Ibcast

MPI_Reduce

MPI_Ireduce

MPI_Allreduce

MPI_Iallreduce

MPI_Alltoall

MPI_Ialltoall

MPI_Barrier

MPI_Ibarrier

MPI_Comm_split

MPI_Comm_dup

MPI_Comm_create

MPI_Gather

MPI_Igather

MPI_Allgather

MPI_Iallgather

MPI_Scatter

## Notes

* Linux Only
* Works with openmpi or Intelmpi, ensure to use corresponding path to mpi.h head file to compile the code.
* Tested on OSU Micro-Benchmarks 5.5 and HPL 2.2
* Tested on mpiGraph
  * ~~there is a bug in mpiGraph, see https://github.com/LLNL/mpiGraph/pull/2/files to fix and run~~ (Already fixed & merged in latest version)





