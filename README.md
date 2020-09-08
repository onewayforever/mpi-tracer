# MPI TRACER
The realtime monitor for tracing the MPI behavior 


## Usage
Add mpitracer.so as the LD_PRELOAD library 
Use -x LD_PRELOAD=/path/to/mpitracer.so during mpirun

Example:

mpirun  --hostfile myhosts -np 64 -npernode 32  -x LD_PRELOAD=/path/to/mpitracer.so /path/to/MPIApp



## Parameters 

MPI tracer use environment variables to pass parameters. The available options are listed below.

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

* MPITRACER_DELAY_WRITER

  MPI trace will create a writer thread to write the logs to the file on each rank process, you can set MPITRACER_DELAY_WRITER=1 to disable writer thread and don't  flush the logs to file during the application runtime util MPI_Finalize stage

Example:

mpirun  --hostfile myhosts -np 64 -npernode 32 -x MPITRACER_TSC_GHZ=2.5 -x MPITRACER_LOG_SIZE=200000 -x MPITRACER_TIMER=GETTIMEOFDAY  -x LD_PRELOAD=/path/to/mpitracer.so /path/to/MPIApp






## Log format
```
       ID                  MPI_TYPE   TimeStamp      Call     Elapse     Comm     Tag     SRC     DST    SCount   SBuf_B   SLen_B SBW_Gbps    RCount   RBuf_B   RLen_B RBW_Gbps
        0                  MPI_Recv    0.411055  0.000021   0.000021  6477824    9001       1       0         0        0        0   0.000         1        4        4   0.002
        1                  MPI_Recv    0.411079  0.000002   0.000002  6477824    9001       2       0         0        0        0   0.000         1        4        4   0.015
```


| Column    | Descrpition                                                  |
| --------- | ------------------------------------------------------------ |
| MPI_TYPE  | the MPI_xxx functions to hook and log, available functions are listed in the next section |
| TimeStamp | seconds since MPI_Init is called                             |
| Call      | the running time of the called function                      |
| Elapse    | if the function is synchronous, eg MPI_Send, it is equal to Call<br />if the function is asynchronous, eg MPI_Isend, it is the time between the asychronous function being called and its asynchronous request being checked positive by MPI_Test or MPI_Wait |
| Comm      | the Commnunicator of the function                                 |
| Tag       | the tag of the function                                           |
| SRC       | the source rank of the function, display -1 if NA                 |
| DST       | the destination rank of the function, display -1 if NA            |
| SCount    | the count of buffers for sending                             |
| SBuf_B    | the size of a single buffer for sending, in Bytes            |
| SLen_B    | the total size of sending buffer, SLen_B = SCount * SCount, in Bytes |
| SBW_Gbps  | the bandwidth of sending process SBW_Gbps = SLen_B/Elapse, formatted to Gbps |
| RCount    | the count of buffers for receiving                           |
| RBuf_B    | the size of a single buffer for receiving, in Bytes          |
| RLen_B    | the total size of receiving buffer, RLen_B = RCount * RBuf_B, in Bytes |
| RBW_Gbps  | the bandwidth of receiving process RBW_Gbps = RLen_B/Elapse, formatted to Gbps |



## MPI Hooks

The available MPI APIs to monitor now

MPI_Send
MPI_Recv
MPI_Isend
MPI_Irecv
MPI_Wait
MPI_Waitall
MPI_Test
MPI_Testall
MPI_Bcast
MPI_Ibcast
MPI_Reduce
MPI_Ireduce







