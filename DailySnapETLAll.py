import traceback




import sys
import os
import pymysql
import time
import datetime
import gc

from datetime import date
from datetime import timedelta
import logging
import logging.config
import json
from glob import glob
import math
import re


#추가 한 패키지 
import argparse
import psutil
from pyarrow import csv
#os.environ["MODIN_ENGINE"] = "ray"  # Modin will use Ray
#import ray
#ray.init()
import modin.pandas as pd
import pandas as pd
import numpy as np

from putil import CsvUtil,DataBaseUtil,DataFrameUtil

import multiprocessing
from multiprocessing import Process, Pool, TimeoutError
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor
import threading


sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))+'/../common')
from connection.n import *
from connection.n import DWConnection as ENNDWConnection
from util.stringUtil import StringUtil
from util.lineWorks import LineWorks


#config = json.load(open('./logger.json'))
#logging.config.dictConfig(config)
#logger = logging.getLogger(__name__)

globalTime = {}
globalTime['main']=  time.time() 
data_frame_tables={}




########################################################
##############  새로짠 스크립트 ########################
########################################################

def get_thread_name():
    thread_name=''
    try:
        #thread_name = threading.currentThread().getName()
        thread_name =  multiprocessing.current_process()
    except Exception as ex:
        print(ex)
    return thread_name

def system_info(key="메모리", target="target"):
    """
        시스템 사용 정보를 보여준다.
    """
    global globalTime
    memory_usage_dict = dict(psutil.virtual_memory()._asdict())
    memory_usage_percent = memory_usage_dict['percent']    
    pid = os.getpid()
    current_process = psutil.Process(pid)
    current_process_memory_usage = current_process.memory_info()[0] / 2.**20
    
    du = psutil.disk_usage(path='/') # 디스크 데이터
    dis_free = du.free//(1024*1024)
    dis_idle = du.free/du.total*100
    if target not in  globalTime:
        globalTime[target] = time.time()

    totalTime = time.time() - globalTime['main']
    targetTime = time.time() - globalTime[target]

    globalTime[target] = time.time()
    print(f"{target} => {key}\t: systerm memory usage(%): {memory_usage_percent}%  process memory: {current_process_memory_usage: 9.3f} MB  >targe time : {targetTime}  total time : {totalTime}  ::: 남음 Disk: {dis_free:,}MB ({dis_idle:.1f}%)")

def target_server(path, servers):
    """
        1. 설정 정보에서 서버 목록 가져오기
        2. 설정 정보 없으면 디렉토리에서 서버 목록 가져오기
         
    """
    if servers : 
        return servers.split(",")
    else:
        servers = list()
        files = os.listdir(path) 
        for name in files :
            if os.path.isdir(path+r"/"+name):
                if name.isdigit():
                    servers.append(name)
        return servers

def etl_porcess_list(region, dataType, periodType, tableList, stattype):
    """
        2. 처리 데이터 가져오기 -> DW
    """
    results = list()
    conn = ENNDWConnection.getDw1Connection()
    query = f"""
        SELECT * 
        FROM 
            TwoWorldsDW.dbo.etl_file_list 
        WHERE 
            CHARINDEX('{region}', regions) > 0
        AND period_type = '{periodType}'        
     """
    if dataType.lower() == 'game':
         query = query +"\n AND countstr is not null"
    elif dataType.lower() == 'open':
        query = query +"\n AND datamart_join_table is not null"
    elif dataType.lower() == 'anomaly':
        query = query +"\n AND anomaly_join_table is not null"
    if tableList:
        tables = tableList.split(",")
        query = query + "\n AND etl_filename in ('" + "','".join(tables)                            
        query = query + "' )"
    if stattype:
        tables = stattype.split(",")
        query = query + "\n AND stat_type in (" + ",".join(tables)                            
        query = query + " )"
    print(query)
    try:
        rows = conn.cursor(as_dict=True)
        rows.execute(query)
        for row in rows :            
            results.append(row)
         
        return results
    except Exception as ex:     
        print('파일 목록을 조회 실패 '+str(ex))
        raise Exception('파일 목록을 조회 실패 '+str(ex))            
    finally:
        if conn :
            conn.close()


def get_file_path(base_path, server_id, date):
    """
        물리적 파일 위치를 찾는다 ( 규칙 ) 
    """
    return f"{base_path}/{server_id}/{date}/"


def value_zero_check(x) :
  if str(x).isdigit() :
    return x
  return 0

def null_to_zero(df):    
    change_type = {}
    for column in df.columns:
        column_type = DataFrameUtil.columns_type(column, df[column].dtypes)
        if column_type:
            df[column]=df[column].apply(value_zero_check)
            change_type[column]= column_type

    return df.astype(change_type)

def type_change(df, file_name, stat_type):
    try:
        if 'battlefield_relic_detail_log' == file_name and stat_type:
            return df.astype({'param3':'int64'})
        else:
            return df
    except Exception as ex:     
        print(f'type_change error file_name: {file_name}, stat_type : {stat_type}')
    return df



def csv_to_data_frame(args, path, file_name, etl_info, j_info, a_info):
    """
        csv 파일을 데이터 프레임으로 전환한다.
    """    
    csv_path = path + file_name+'.csv'
    if not os.path.exists(csv_path):
        csv_path = path + file_name+'.csv.gz'
    print(csv_path)
    #, compression='gzip'
    c_df = None
    try:
        csv_headers = CsvUtil.csv_header(csv_path)
        df_types = DataFrameUtil.columns_data_type(csv_headers)
        df_in_columns = DataFrameUtil.table_loding_columns(file_name,csv_headers, etl_info, j_info, a_info)
        opts = csv.ConvertOptions(column_types = df_types, 
                                  include_columns = df_in_columns, 
                                  strings_can_be_null = True,
                                  )
        c_df = csv.read_csv(csv_path,convert_options=opts).to_pandas()
        c_df = DataFrameUtil.table_where(c_df, etl_info['wherestr'])
        print(c_df.sample(frac=0.2,replace=True))
        #데이터 가공
        c_df = c_df.fillna(0)
        c_df = null_to_zero(c_df)
        c_df = type_change(c_df, file_name, etl_info['stat_type'])
        
        #정보 출력
        print("#########"*10)
        print(c_df.info())
        print("--------"*10)
        print(c_df.sample(frac=0.2,replace=True))
        print("#########"*10)
        return c_df
    except Exception as exc:            
        raise Exception('csv_to_data_frame 데이터 프레임 생성 실패 : '+ str(exc))
        


def target_df(args, server_id, etl_info, j_info, a_info):
    """
        기본 DF 파일을 만든다.
    """
    t_df = None
    try:
        if args.iscsv : 
            file_path = get_file_path(args.targetpath, server_id, args.date)
            t_df = csv_to_data_frame(args, file_path, etl_info['etl_filename'], etl_info, j_info, a_info)
        else:
            print('TODO 직접DB 조회하는 부분 개발해야함')
    except Exception as exc:            
        raise Exception(f'target_df 만들기 실패 {etl_info["etl_filename"]}: '+ str(exc))

    return t_df



def join_df(args, server_id, etl_info, j_info, a_info):
    """
        join 테이블을 로딩한다.
    """
    j_df = None    
    try:
        if not j_info : 
            raise Exception('etl_info 에서 조인테이블 정보를 찾을 수 없습니다.')
        if j_info['join_table'] == None:
            j_df = pd.DataFrame()
        else:
            file_path = get_file_path(args.targetpath, server_id, args.date)
            j_df = csv_to_data_frame(args, file_path, j_info['join_table'], etl_info, j_info, a_info)        
    except Exception as exc:            
        raise Exception(f'join_df 만들기 실패 {j_info["join_table"]}: '+ str(exc))
    return j_df
def asset_df(args, server_id,etl_info, j_info, a_info):
    """
        join 테이블을 로딩한다.
    """
    a_df = None
    try:        
        if not a_info : 
            return None
        file_path = get_file_path(args.targetpath, server_id, args.date)
        a_df = csv_to_data_frame(args, file_path, a_info['join_table'], etl_info, j_info, a_info)        
    except Exception as exc:            
        raise Exception(f'asset_df 만들기 실패 {a_info["join_table"]}: '+ str(exc))
    return a_df



def generator_dataframe(args, server_id, etl_info, j_info, indexText):
    """
       기본 데이터 프레임을 만든다 계산 하기전 원천 데이터
    """  
    thread_name = get_thread_name()
    a_info = DataFrameUtil.asset_info(etl_info)
    t_df = target_df(args, server_id, etl_info, j_info, a_info) # 대상 정보    
    print(f"{thread_name} {indexText} 파일별 실행 1/8 {etl_info['etl_filename']} - {j_info['join_table']}\t")   
    j_df = join_df(args, server_id, etl_info, j_info, a_info)    
    print(f"{thread_name} {indexText} 파일별 실행 2/8 {etl_info['etl_filename']}- {j_info['join_table']} 데이터 머지\t\t\t\t")
    t_df = DataFrameUtil.merge_dataframe(t_df, j_df, j_info)
    print(f"{thread_name} {indexText} 파일별 실행 3-1/8 {etl_info['etl_filename']}- {j_info['join_table']} 에셋 로딩\t\t\t\t\t")    
    a_df = asset_df(args, server_id, etl_info, j_info, a_info)
    if isinstance(a_df, pd.DataFrame):        
        print(f"{thread_name} {indexText} 파일별 실행 3-2/8  {etl_info['etl_filename']}- {j_info['join_table']} - {etl_info['asset_table']}  에셋 머지\t\t\t\t")
        t_df = DataFrameUtil.merge_dataframe(t_df, a_df, a_info)
        

    return t_df


def where_str(data_type,etl_info):
    if data_type.lower() =='game':
        return etl_info['wherestr']
    elif data_type.lower() =='open':
        return etl_info['open_wherestr']
    else :
        return etl_info['wherestr']

def etl_process(data_type, t_df, etl_info, j_info, indexText):
    """
        데이터 처리
    """
    thread_name = get_thread_name()
    #필터 처리
    where_query = where_str(data_type, etl_info)
    t_df = DataFrameUtil.where_dataframe(t_df, where_query)
    print(f"{thread_name} {indexText} 파일별 실행 5/8 ceil\t\t\t\t")
    #그룹 정보 가져오기    
    t_df = DataFrameUtil.create_ceil_column(t_df, j_info['group'])
    print(f"{thread_name} {indexText} 파일별 실행 6/8 group by\t\t\t\t\t")
    t_df = DataFrameUtil.group_dataframe(t_df,j_info)
    print(f"{thread_name} {indexText} 파일별 실행 7/8 rename \t\t\t\t\t")
    t_df =  DataFrameUtil.rename_column(t_df)




    return t_df

def thread_process(args,server_id, etl_info, db_info, indexText):
    thread_name = get_thread_name()    
    print(f'{thread_name} {indexText} 집계 시작 정보 \t server_id : {server_id}, etl_filename : {etl_info["etl_filename"]}, table_id : {etl_info["table_id"]}, stat_type : {etl_info["stat_type"]} ')
    t_df=pd.DataFrame()
    try:
        j_info = DataFrameUtil.join_info(args.datatype, etl_info)
        print(f"{thread_name} {indexText} join table : {j_info['join_table']}");
        print(f"{thread_name} {indexText} 파일별 실행 0/8 - 타켓 파일 읽는중 - {etl_info['etl_filename']} ")
        t_df = generator_dataframe(args, server_id, etl_info, j_info, indexText)
        print(f"{thread_name} {indexText} 파일별 실행 4/8 where 처리 - {etl_info['etl_filename']} \t\t\t\t\t\t")
        t_df = etl_process(args.datatype, t_df, etl_info, j_info, indexText)
        print(f"{thread_name} {indexText} 파일별 실행 7/8 - {etl_info['etl_filename']} 종료")        
        DataBaseUtil.save_df(args,server_id, etl_info, db_info, j_info, t_df)#TODO DW에 실행 시간 업데이트 한다.
    except Exception as ex:
        error_message  = f'{thread_name} - [ ERROR ] 집계 처리 실패\t :  server_id : {server_id}, etl_filename : {etl_info["etl_filename"]}, table_id : {etl_info["table_id"]},stat_type : {etl_info["stat_type"]},error_message : {ex}'
        if server_id[-3:] =='001':
            LineWorks.send('juyong.park@nm-neo.com,haeun@nm-neo.com,sangchul@nm-neo.com', error_message)
        print(error_message)
    finally:
        del [[t_df]]
        gc.collect()
        t_df=pd.DataFrame()
    ###메모리 해제####
    system_info(f"{thread_name} - 프로세스 종료 {server_id}", "ETL")
    
    


        





def etl_process_server(args,servers, etl_list, db_info):
    """
      순차적으로 프로세스 돌리기(1 서버)
      > 여기서 프로세스 정보를 가지고  Thread  할지 순차 프로그램 할지 구성한다. ( 일단 순차 프로그래밍 )
        #1. 파일로딩 한다.
        #2. 조인 테이블 로딩
        #3. 에셋 테이블 로딩
        #4. 집계 처리
    """
    system_info(f"집계 데이터 처리", "ETL")
    count = 0
    if args.isthread =='Y' :
        
        with ProcessPoolExecutor(args.threadcount) as executor :
            for server_id in servers:                
                for index, etl_info in enumerate(etl_list):
                    executor.submit(thread_process, args, server_id, etl_info,db_info, str(count)+"/"+str(len(etl_list)*len(servers)))
                    count = count+1
            
    else:
        for server_id in servers:
            for index, etl_info in enumerate(etl_list): 
                thread_process(args, server_id, etl_info,db_info,  str(count)+"/"+str(len(etl_list)*len(servers)))
                count = count+1
    system_info(f"집계 데이터 처리", "ETL")
    

def get_db_info(location):
    ct = ConnectionUtil(0)
    ananInfo = ct.getAnalyticsLocaleInfos(location)
    #print(ananInfo)
    return ananInfo[0]



if __name__ == '__main__':
    """
    TODO 해당 디렉토리의 디렉토리 정보에서 서버 ID 값을 가지고온다.
    ETL파일리스트를 가지고 와서 데이터 처리
    순차 적으로 처리 하면서 메모리 체크 
    """ 
    parser = argparse.ArgumentParser()
    parser.add_argument('--gamecode', type=str, help="게임코드[ENN, ENNT]")
    parser.add_argument('--location', type=str, help="로컬[KOREA, TAIWAN]")
    parser.add_argument('--region', type=str, help="리전[KOREA, JAPAN, TAIWAN]")
    parser.add_argument('--datatype', type=str, help="유저 구분 종류[open, game, anomaly] 넷마블 유저 구분, DB기반, 제외 유저")
    parser.add_argument('--targetpath', type=str, help="파일이 저장 되어 있는 위치")
    parser.add_argument('--date', type=str, default=((datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')), help="실행되는날짜")
    parser.add_argument('--iscsv', type=str, default="Y", help="대상정보를 CSV할껀지 DB 직접 조회 할껀지 체크")
    parser.add_argument('--tablelist', type=str, default=None, help="집계할 테이블 리스트 ,로 분리 한다.")
    parser.add_argument('--stattype', type=str, default=None, help="집계할 테이블 리스트 ,로 분리 한다.")
    parser.add_argument('--periodtype', type=str, default='DAY', help="집계 기간 종류 설정")
    parser.add_argument('--servers', type=str, default=None, help="집계할 서버 정보")
    parser.add_argument('--isthread', type=str, default='Y', help="thread 여부")
    parser.add_argument('--threadcount', type=int, default=8, help="thread 갯수")
    parser.add_argument('--istemp', type=str, default='', help="Temp 명칭 ''|'temp'")
    
    args = parser.parse_args()
    print(args)
    #>>>> 처리 프로세스 순서 <<<<
    # 1. 디렉토리에서 서버 목록 가져오기
    # 2. 처리 데이터 가져오기 -> DW
    # 3. 순차적으로 프로세스 돌리기(1 서버 > 프로세스 리스트 ) 
    #    3.1 기존 저장되어있는 DB 정보 지우기
    #    3.2 프로세스 돌려 집계 구하기
    #       3.2.1 메인 파일 로딩 하기
    #       3.2.2 조인 테이블 종류에 맞게 로딩하기
    #       3.2.3 집계 함수 돌리기
    #    3.3 프로세스 내용 저장하기
    # 4. 에러 없이 성공이면 파일지우기?    

    

    servers = target_server(args.targetpath, args.servers) # 1. 디렉토리에서 서버 목록 가져오기
    etl_list = etl_porcess_list(args.region, args.datatype, args.periodtype, args.tablelist, args.stattype) # 2. 처리 데이터 가져오기 -> DW  
    db_info = get_db_info(args.location)
    
    print(str(servers))
    etl_process_server(args,servers, etl_list,db_info)
    #for server_id in servers: # 3. 순차적으로 프로세스 돌리기 ( 서버 ) 
    #    system_info(f"서버 별 프로세스 시작 {server_id}", "server")
    #    etl_process_server(args,server_id, etl_list,db_info)
    #    system_info(f"서버 별 프로세스 종료 {server_id}", "server")
    #print(str(errorInfo))