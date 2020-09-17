import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import argparse
import os

def traffic_matrix_by_rank(df,option):
    total_rank=max(df.SRC.max(),df.SRC.max())+1
    mat=np.zeros([total_rank,total_rank])
    for index, row in df.iterrows():
        mat[row["SRC"],row["DST"]]=row[option]
    return mat

def traffic_matrix_by_host(df,option):
    total_rank=max(df.SHost.max(),df.SRC.max())+1
    mat=np.zeros([total_rank,total_rank])
    for index, row in df.iterrows():
        mat[row["SRC"],row["DST"]]=row[option]
    return mat



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
    plt.matshow(mat)
    plt.savefig(shotname+"_{}.png".format(option))
    print('save to ' + './'+shotname+"_{}.png".format(option))

