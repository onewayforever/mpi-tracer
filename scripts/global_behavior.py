import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import argparse
import os

regular={
    'TotalBytes':100000000,
    'Bw_mean':0.5,
    'Max_mean':0.5,
    'Min_mean':0.5,
    'Avg_mean':0.5,
}

def traffic_matrix_by_rank(df,option):
    total_rank=max(df.SRC.max(),df.SRC.max())+1
    mat=np.zeros([total_rank,total_rank])
    for index, row in df.iterrows():
        mat[row["SRC"],row["DST"]]=row[option]
    return mat

def traffic_matrix_by_host(df,option):
    hosts=list(set(df.SHost)|set(df.DHost))
    hosts=list(sorted(hosts))
    total_hosts=len(hosts)
    mat=np.zeros([total_hosts,total_hosts])
    mat_start=np.ones([total_hosts,total_hosts])*99999
    mat_end=np.zeros([total_hosts,total_hosts])


    for index, row in df.iterrows():
        #print(row["SRC"], row["DST"])
        #mat[row["SRC"],row["DST"]]=row['Bw_mean']
        #mat[row["SRC"],row["DST"]]=row['TotalBytes']
        #mat[row["SRC"],row["DST"]]=row[option]
        i=hosts.index(row["SHost"])
        j=hosts.index(row["DHost"])
        mat[i,j]+=row[option]
        if row['Start'] < mat_start[i,j]:
            mat_start[i,j]=row['Start'] 
        if (row['Start']+row['Elapse']) > mat_end[i,j]:
            mat_end[i,j]=row['Start']+row['Elapse'] 
    if option=='TotalBytes':
        return mat
    if option=='Avg_bw':
        return mat/(mat_end-mat_start)*8*1e-9


if __name__=="__main__":
    parser = argparse.ArgumentParser(prog='global_behavior')
    parser.add_argument('file',help='Task summary log file to display')
    #parser.add_argument('option',help='Task summary log file to display')
    parser.add_argument('--by',choices=['rank','host'], help='View the relationship among hosts or ranks,defaut by ranks')
    parser.add_argument('option', choices=['Bw_mean','Max_bw','Min_bw','Avg_bw','TotalBytes'],help='Behavior to display')
    args = parser.parse_args()

    log="/Users/wanw/Downloads/mpi_trace_task_0917115810.log"
    log = args.file
    if log is None:
        print('Need task summary log to parse')
        exit(0)
    option = args.option
    if option is None:
        print('Need option to parse')
        exit(0)
    by = args.by
    if by is None:
        by='rank'
    df=pd.read_csv(log,sep='\\s+')
    if by == 'rank':
        mat=traffic_matrix_by_rank(df,option)
    if by == 'host':
        if not (option in ['Avg_bw','TotalBytes']):
            print('Only support Avg_bw,TotalBytes by hosts')
            exit(0)
        mat=traffic_matrix_by_host(df,option)
    filepath,tempfilename = os.path.split(log);
    shotname,extension = os.path.splitext(tempfilename);
    mat=np.log(mat)
    #mat=np.log(mat-regular[option])
    plt.matshow(mat)
    plt.savefig(shotname+"_{}_{}.png".format(by,option))
    print('save to ' + './'+shotname+"_{}_{}.png".format(by,option))

