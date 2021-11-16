# -*- coding: utf-8 -*-

import pandas as pd

import sys
import os
import time
import datetime

from datetime import date
from datetime import timedelta

import json

import logging
import logging.config



config = json.load(open(os.path.abspath(os.path.dirname(__file__))+'/logger.json'))
logging.config.dictConfig(config)

logger = logging.getLogger(__name__)


print(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))+'/../common')
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))+'/../common')

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=os.path.dirname(os.path.abspath(os.path.dirname(__file__)))+'/../common' + '/netmarble-neo-55ce1f8dff99.json';



from connection.n import *
#from util.stringUtil import StringUtil
#from util.lineWorks import LineWorks


from google.cloud import bigquery

from glob import glob
from multiprocessing import Process, Queue, Pool




ct = ConnectionUtil(0)
lobby_conn = LobbyConnection(0)


systemStat = 1

data_hour = None


def get_work(vday, vserverID , vhost, vport, vuser, vpass, vdbName, vtableName, betweenDay, columnList, createFolderName, dbType, data_hour=None):
    start_datetime = GetCurrentDateTime()



    try:

        # gameDB payment_log 에외 처리
        if vtableName == 'payment_log':
            betweenDay = 1


        # betweenDay : 0 No day, 1 over day
        if betweenDay == 0:

            tableQueryStr = """
            SELECT  {columnList}
            FROM	`{tableName}`;
            """.format(tableName=vtableName,columnList=columnList)

            #print (tableQueryStr)

            df=ct.getQueryDF(vhost, vport, vuser, vpass, vdbName, tableQueryStr)

        # log
        elif betweenDay == 1:



            list_exception = ['common_log','asset_log','character_detail_log','item_log','npc_reward_detail_log']
            if vtableName in list_exception:
                if data_hour is not None:

                    n = datetime.datetime.strptime(vday, '%Y-%m-%d')
                    n = n + datetime.timedelta(hours=int(data_hour))
                    startDayID = (int(time.mktime(n.timetuple())) << 22) * 1000

                    p_month = int(n.month)

                    n = n + datetime.timedelta(hours=1)
                    endDayID = (int(time.mktime(n.timetuple())) << 22) * 1000 - 1
                else:
                    return

            else:
                n = datetime.datetime.strptime(vday, '%Y-%m-%d')
                startDayID = (int(time.mktime(n.timetuple())) << 22) * 1000

                p_month = int(n.month)

                n = n + datetime.timedelta(days=betweenDay)
                endDayID = (int(time.mktime(n.timetuple())) << 22) * 1000 - 1

            tableQueryStr = """
            SELECT  {columnList}
            FROM	`{tableName}`
            WHERE   id >= {startDayID}
            AND     id <= {endDayID}
            AND     p_month = {p_month};
                """

            print(tableQueryStr.format(tableName=vtableName,startDayID=startDayID,endDayID=endDayID,columnList=columnList,p_month=p_month))
            df = ct.getQueryDF(vhost, vport, vuser, vpass, vdbName, tableQueryStr.format(tableName=vtableName,startDayID=startDayID,endDayID=endDayID,columnList=columnList,p_month=p_month))

        # WEEK
        elif betweenDay == 201:


            n = datetime.datetime.strptime(vday, '%Y-%m-%d')
            start_date = n - datetime.timedelta(days=n.weekday())
            startDayID = (int(time.mktime(start_date.timetuple())) << 22) * 1000


            end_date = n + datetime.timedelta(days=7 - n.weekday())
            endDayID = (int(time.mktime(end_date.timetuple())) << 22) * 1000 - 1

            tableQueryStr = """
            SELECT  {columnList}
            FROM	`{tableName}`
            WHERE   id >= {startDayID}
            AND     id <= {endDayID};
            """

            df = ct.getQueryDF(vhost, vport, vuser, vpass, vdbName, tableQueryStr.format(tableName=vtableName,startDayID=startDayID,endDayID=endDayID,columnList=columnList))

        # MONTH
        elif betweenDay == 301:

            n = datetime.datetime.strptime(vday, '%Y-%m-%d')
            start_date = datetime.date(n.year,n.month,1)
            startDayID = (int(time.mktime(start_date.timetuple())) << 22) * 1000

            end_date = pd.Period(n, freq='M').end_time.date()
            end_date = end_date + datetime.timedelta(days=1)
            endDayID = (int(time.mktime(end_date.timetuple())) << 22) * 1000 - 1

            tableQueryStr = """
            SELECT  {columnList}
            FROM	`{tableName}`
            WHERE   id >= {startDayID}
            AND     id < {endDayID};
            """

            df = ct.getQueryDF(vhost, vport, vuser, vpass, vdbName, tableQueryStr.format(tableName=vtableName,startDayID=startDayID,endDayID=endDayID,columnList=columnList))

        elif betweenDay == 401:


            n = datetime.datetime.strptime(vday, '%Y-%m-%d')
            start_date = pd.Period(n, freq='Q').start_time.date()
            startDayID = (int(time.mktime(start_date.timetuple())) << 22) * 1000

            end_date = pd.Period(n, freq='Q').end_time.date()
            end_date = end_date + datetime.timedelta(days=1)
            endDayID = (int(time.mktime(end_date.timetuple())) << 22) * 1000 - 1

            tableQueryStr = """
            SELECT  {columnList}
            FROM	`{tableName}`
            WHERE   id >= {startDayID}
            AND     id < {endDayID};
            """

            df = ct.getQueryDF(vhost, vport, vuser, vpass, vdbName, tableQueryStr.format(tableName=vtableName,startDayID=startDayID,endDayID=endDayID,columnList=columnList))

        elif betweenDay == 501:


            n = datetime.datetime.strptime(vday, '%Y-%m-%d')
            start_date = pd.Period(n, freq='Y').start_time.date()
            startDayID = (int(time.mktime(start_date.timetuple())) << 22) * 1000

            end_date = pd.Period(n, freq='Y').end_time.date()
            end_date = end_date + datetime.timedelta(days=1)
            endDayID = (int(time.mktime(end_date.timetuple())) << 22) * 1000 - 1

            tableQueryStr = """
            SELECT  {columnList}
            FROM	`{tableName}`
            WHERE   id >= {startDayID}
            AND     id < {endDayID};
            """

            df = ct.getQueryDF(vhost, vport, vuser, vpass, vdbName, tableQueryStr.format(tableName=vtableName,startDayID=startDayID,endDayID=endDayID,columnList=columnList))


        #stat
        elif betweenDay == 101:

            n = datetime.datetime.strptime(vday, '%Y-%m-%d') + datetime.timedelta(days=1)
            strEnddate = n.strftime('%Y-%m-%d')

            pre14StartDay = n + datetime.timedelta(days=-14)

            n = n + datetime.timedelta(days=-(betweenDay-100))
            strStartdate = n.strftime('%Y-%m-%d')

            if vtableName == 'nru':
                tableQueryStr = """
                SELECT  {columnList}
                FROM	`{tableName}`
                WHERE   registered_time >= '""" + str(pre14StartDay) + """ '
                AND     registered_time < '{strEnddate}';
                """

            elif vtableName == 'cu':
                tableQueryStr = """
                SELECT  {columnList}
                FROM	`{tableName}`
                WHERE   log_date >= '{strStartdate}'
                AND     log_date < '{strEnddate}';
                """
            else:

                tableQueryStr = """
                SELECT  {columnList}
                FROM	`{tableName}`
                WHERE   id_date >= '{strStartdate}'
                AND     id_date < '{strEnddate}';
                """

            df = ct.getQueryDF(vhost, vport, vuser, vpass, vdbName, tableQueryStr.format(tableName=vtableName,strStartdate=strStartdate,strEnddate=strEnddate,columnList=columnList))

        day_str = ['WEEK', 'MONTH', 'QUARTER' , 'YEAR']
        for row in day_str:
            if row in dbType:
                csvFileName = str(vtableName).lower() + '_' + row + '.csv'
                break
            else:
                csvFileName = str(vtableName).lower() + '.csv'


        #df = df.drop_duplicates()

        if data_hour is None:
            csvFileName = vtableName + '.csv'
        else:
            csvFileName = vtableName + '_' + str(data_hour) + '.csv'



        df.to_csv(createFolderName + "/" + csvFileName, sep=',',index=False)

        end_datetime = GetCurrentDateTime()
        DWConnection.set_dw1_backup_log('SP_BACKUP_TASK_LOG_C',
                                        (vtableName, start_datetime, end_datetime))
    except Exception as ex:
        systemStat = systemStat - 1
        print('[ETL] 에러가 발생 했습니다', ex)
        logger.error("vhost = " + str(vhost) + ", dbType = " + str(dbType) + ", vserverID = " + str(vserverID) + " : " + str(ex))



    return






def get_biglog(vday, vserverID , vhost, vport, vuser, vpass, vdbName, vtableName, betweenDay, columnList, createFolderName, dbType, data_hour=None):
    start_datetime = GetCurrentDateTime()


    try:

        # log
        if betweenDay == 1:

            n = datetime.datetime.strptime(vday, '%Y-%m-%d')
            startDayID = (int(time.mktime(n.timetuple())) << 22) * 1000

            p_month = int(n.month)

            n = n + datetime.timedelta(days=betweenDay)
            endDayID = (int(time.mktime(n.timetuple())) << 22) * 1000 - 1

            if int(vserverID) < 20003001:
                vuser = "enn_mng"
                vpass = "R29RrGa9P+hZB^_E"
            else:
                vuser = "ennt_mng"
                vpass = "b@z=gaLQUh7eZ^Qz"

            str_mysqldump = "mysqldump --default-character-set=utf8mb4 --single-transaction --force --opt -h {vhost} -P {vport} -u {vuser} -p{vpass} {vdbName} {vtableName} --where=\"id>={startDayID} and id <= {endDayID}  \" > {createFolderName}/{vtableName}.sql"
            str_mysqldump = str(str_mysqldump).format(vhost=vhost,vport=vport,vuser=vuser,vpass=vpass,vdbName=vdbName,vtableName=vtableName,startDayID=startDayID,endDayID=endDayID,createFolderName=createFolderName)

            str_mysqldump_to_csv = "python ./mysqldump_to_csv.py {createFolderName}/{vtableName}.sql > {createFolderName}/{vtableName}.tmp"
            str_mysqldump_to_csv = str_mysqldump_to_csv.format(vtableName=vtableName,createFolderName=createFolderName)


            str_mysql = "mysql -h {vhost} -P {vport} -u {vuser} -p{vpass} -D{vdbName} -e \"SELECT COLUMN_NAME FROM information_schema.COLUMNS C WHERE TABLE_SCHEMA = '{vdbName}' AND table_name = '{vtableName}'  ORDER BY ordinal_position ;\" | grep -iv ^COLUMN_NAME$ | sed 's/^/\"/g;s/$/\"/g' | tr '\\n' ','  | sed 's/.$//' > '{createFolderName}/{vtableName}.obj'"
            str_mysql = str_mysql + "; echo -e '\n' >> {createFolderName}/{vtableName}.obj"
            str_mysql = str_mysql.format(vhost=vhost,vport=vport,vuser=vuser,vpass=vpass,vdbName=vdbName,vtableName=vtableName,createFolderName=createFolderName)

            str_cat = " cat  {createFolderName}/{vtableName}.obj {createFolderName}/{vtableName}.tmp > {createFolderName}/{vtableName}.csv"
            str_cat = str_cat.format(vtableName=vtableName,createFolderName=createFolderName)

            f = open("run_biglog_{vtableName}_{vserverID}.sh".format(vtableName=vtableName,vserverID=str(vserverID)), 'w')
            f.write(str_mysqldump + ';\n')
            f.write(str_mysqldump_to_csv + ';\n')
            f.write(str_mysql + ';\n')
            f.write(str_cat + ';\n')
            #if vtableName =='common_log' : 
            #    f.write("mv "+createFolderName+"/common_log.csv "+createFolderName+"/common_log_original.csv ;\n")
            #    f.write("awk -F ',' '$7 != 111252||$7 =='trid_info' {print $0}' "+createFolderName+"/common_log_original.csv > "+createFolderName+"/common_log.csv" + ';\n')
            f.close()



        #csvFileName = str(vtableName).lower() + '.csv'


        #df.to_csv(createFolderName + "/" + csvFileName, sep=',')

        end_datetime = GetCurrentDateTime()
        DWConnection.set_dw1_backup_log('SP_BACKUP_TASK_LOG_C',
                                        (vtableName, start_datetime, end_datetime))
    except Exception as ex:
        systemStat = systemStat - 1
        print('[ETL] 에러가 발생 했습니다', ex)
        logger.error("vhost = " + str(vhost) + ", dbType = " + str(dbType) + ", vserverID = " + str(vserverID) + " : " + str(ex))



    return





def get_game_full(vday, vserverID , vhost, vport, vuser, vpass, vdbName, vtableName, betweenDay, columnList, createFolderName, dbType, data_hour=None):
    start_datetime = GetCurrentDateTime()


    try:

        # GAME
        if betweenDay == 0:


            if int(vserverID) < 20003001:
                vuser = "enn_mng"
                vpass = "R29RrGa9P+hZB^_E"
            else:
                vuser = "ennt_mng"
                vpass = "b@z=gaLQUh7eZ^Qz"

            str_mysqldump = "mysqldump --default-character-set=utf8mb4 --force --opt -h {vhost} -P {vport} -u {vuser} -p{vpass} {vdbName} {vtableName}  > {createFolderName}/{vtableName}_full.sql"
            str_mysqldump = str(str_mysqldump).format(vhost=vhost,vport=vport,vuser=vuser,vpass=vpass,vdbName=vdbName,vtableName=vtableName,createFolderName=createFolderName)

            str_mysqldump_to_csv = "python ./mysqldump_to_csv.py {createFolderName}/{vtableName}_full.sql > {createFolderName}/{vtableName}_full.tmp"
            str_mysqldump_to_csv = str_mysqldump_to_csv.format(vtableName=vtableName,createFolderName=createFolderName)


            str_mysql = "mysql -h {vhost} -P {vport} -u {vuser} -p{vpass} -D{vdbName} -e \"SELECT COLUMN_NAME FROM information_schema.COLUMNS C WHERE TABLE_SCHEMA = '{vdbName}' AND table_name = '{vtableName}'  ORDER BY ordinal_position ;\" | grep -iv ^COLUMN_NAME$ | sed 's/^/\"/g;s/$/\"/g' | tr '\\n' ','  | sed 's/.$//' > '{createFolderName}/{vtableName}_full.obj'"
            str_mysql = str_mysql + "; echo -e '\n' >> {createFolderName}/{vtableName}_full.obj"
            str_mysql = str_mysql.format(vhost=vhost,vport=vport,vuser=vuser,vpass=vpass,vdbName=vdbName,vtableName=vtableName,createFolderName=createFolderName)

            str_cat = " cat  {createFolderName}/{vtableName}_full.obj {createFolderName}/{vtableName}_full.tmp > {createFolderName}/{vtableName}_full.csv"
            str_cat = str_cat.format(vtableName=vtableName,createFolderName=createFolderName)

            f = open("run_full_{vtableName}_{vserverID}.sh".format(vtableName=vtableName,vserverID=str(vserverID)), 'w')
            f.write(str_mysqldump + ';\n')
            f.write(str_mysqldump_to_csv + ';\n')
            f.write(str_mysql + ';\n')
            f.write(str_cat + ';\n')
            #if vtableName =='common_log' :
            #    f.write("mv "+createFolderName+"/common_log.csv "+createFolderName+"/common_log_original.csv ;\n")
            #    f.write("awk -F ',' '$7 != 111252||$7 =='trid_info' {print $0}' "+createFolderName+"/common_log_original.csv > "+createFolderName+"/common_log.csv" + ';\n')
            f.close()
            os.system("chmod +x run_full_{vtableName}_{vserverID}.sh".format(vtableName=vtableName,vserverID=str(vserverID)))
            os.system("./run_full_{vtableName}_{vserverID}.sh".format(vtableName=vtableName, vserverID=str(vserverID)))



        #csvFileName = str(vtableName).lower() + '.csv'


        #df.to_csv(createFolderName + "/" + csvFileName, sep=',')

        end_datetime = GetCurrentDateTime()
        DWConnection.set_dw1_backup_log('SP_BACKUP_TASK_LOG_C',
                                        (vtableName, start_datetime, end_datetime))
    except Exception as ex:
        systemStat = systemStat - 1
        print('[ETL] 에러가 발생 했습니다', ex)
        logger.error("vhost = " + str(vhost) + ", dbType = " + str(dbType) + ", vserverID = " + str(vserverID) + " : " + str(ex))



    return


def get_opendata(vday, game_code, vserverID , tableName, columnList, createFolderName):

    start = time.time()

    #client = BigDataConnection.getBigQueryConnection("KOREA")
    client = bigquery.Client()
    if str(game_code).upper() == 'ENN':
        data_set_name = 'bis_opendata_enn'
    else:
        data_set_name = 'bis_opendata_ennt'



    query = """
        SELECT  {columnList} 
        FROM    `bi-service-155107.{data_set}.{tableName}`
        WHERE   basis_dt = '{vday}' 
        AND     world_id = {vserverID}
    """

    if tableName == 'v_unvs_fd_users_sale_acc':
        query = """
            SELECT  {columnList} 
            FROM    `bi-service-155107.{data_set}.{tableName}`
            WHERE   basis_dt = '{vday}' 
            AND     user_div_cd = 'PID'
        """

    if tableName == 'v_unvs_fd_bot_mst':
        query = """
            SELECT  {columnList} 
            FROM    `bi-service-155107.{data_set}.{tableName}`
            WHERE   bot_basis_dt = '{vday}' 
        """

    try :
        df = (
            client.query(query.format(columnList=columnList,data_set=data_set_name,tableName=tableName,vday=vday,vserverID=vserverID))
                .result()
                .to_dataframe()
        )

        csvFileName = str(tableName).lower() + '.csv'

        df.to_csv( createFolderName + "/"  + csvFileName, sep=',')

        try :
            file_size = os.stat(createFolderName + "/"  + csvFileName)
            print(createFolderName + "/"  + csvFileName,' > File Size is', file_size.st_size, 'bytes')
        except Exception as ex:
            print(ex)

        print("time :", time.time() - start)

        return

    except Exception as ex:
        print(ex)
    

    










def get_crash(vday, vserverID , tableName, columnList, createFolderName):

    start = time.time()

    client = BigDataConnection.getBigQueryConnection("KOREA")

    query = """
        SELECT  {columnList} 
        FROM    `nm-prod-asne3-crashreport.netmarble_neo.{tableName}` 
        WHERE   eventTimestamp BETWEEN '{vday}' AND '{vday} 23:59:59'
    """


    df = (
        client.query(query.format(columnList=columnList,tableName=tableName,vday=vday,vserverID=vserverID))
            .result()
            .to_dataframe()
    )

    csvFileName = tableName + '.csv'

    df.to_csv( createFolderName + "/"  + csvFileName, sep=',')

    print("time :", time.time() - start)

    return


def GetCurrentDateTime():
	return str((datetime.datetime.utcnow() + datetime.timedelta(hours=9)).date()) + " " + (datetime.datetime.utcnow() + datetime.timedelta(hours=9)).time().strftime("%H:%M:%S")

"""
컬럼 정보를 가져와서 select clause를 만든다.
"""
def get_select_column(vhost, vport, vuser, vpass, vdbName, vtableName, exceptionList):
    queryStr = f"""
            SELECT 
	            TABLE_NAME, COLUMN_NAME 

            FROM 
	            information_schema.columns 
	
            WHERE 
	            TABLE_SCHEMA='{vdbName}' 
	            AND TABLE_NAME='{vtableName}'
;
            """

    #쿼리에서 제외할 컬럼
    exceptColumns = str(exceptionList).split()

    print(f"""get_select_column : {vtableName}  >  {exceptColumns}""")

    df=ct.getQueryDF(vhost, vport, vuser, vpass, vdbName, queryStr)

    df = df[~df.COLUMN_NAME.isin(exceptColumns)]

    return ",".join(map(str, df['COLUMN_NAME'].values));


def main_function(vserver_id, db_type, game_code, location, region, vday, table_name=None, data_hour = None):


    pool = Pool(processes=3)

    localeGameDB = ct.getServers(location)

    createFolderName = os.getcwd() + "/" + location + "/" + region + "/" + vserverID + "/" + str(vday)

    try:
        if not os.path.exists(createFolderName):
            os.makedirs(createFolderName)
    except OSError:
        print('Error: Creating directory. ' + createFolderName)

    jobs = []

    for row in localeGameDB[region]:

        if int(row['id']) == int(vserverID):
            # backup list
            query = "SELECT * FROM TwoWorldsDW.dbo.etl_backup_list"

            backupResults = DWConnection.getDW1Query(query)

            if dbType != "ANALYTICS":

                # DB All Tables
                listTable = ct.getAllTableList(location, region, vserverID, dbType)

                if table_name is not None:
                    listTable = [{'TABLE_NAME': table_name}]

                #print(backupResults)

                for subRow in listTable:

                    backupYes = True

                    columnList = "*"
                    exceptionList = ""


                    for backupRow in backupResults:

                        if subRow['TABLE_NAME'] == backupRow['table_name']:
                            columnList = backupRow['column_list']
                            backupYes = backupRow['backup_yes']
                            exceptionList = backupRow['exception_column_list']
                            break

                    if dbType == 'GAME' and backupYes:
                        betweenDay = 0

                        if exceptionList != "":
                            columnList = get_select_column(row['slave_game_db_host'], row['slave_game_db_port'],
                                                           row['slave_game_db_user'],
                                                           row['slave_game_db_password'], row['slave_game_db_name'],
                                                           subRow['TABLE_NAME'], exceptionList)

                        pool.apply_async(get_work, args=(vday, vserverID,
                            row['slave_game_db_host'], row['slave_game_db_port'], row['slave_game_db_user'],
                            row['slave_game_db_password'], row['slave_game_db_name'],
                            subRow['TABLE_NAME'], betweenDay, columnList, createFolderName, dbType)
                        )


                        #if(len(backupResults)) == 1:
                        #    break


                    if dbType == 'GAME_FULL' and backupYes:
                        betweenDay = 0

                        if exceptionList != "":
                            columnList = get_select_column(row['slave_game_db_host'], row['slave_game_db_port'],
                                                           row['slave_game_db_user'],
                                                           row['slave_game_db_password'], row['slave_game_db_name'],
                                                           subRow['TABLE_NAME'], exceptionList)

                        pool.apply_async(get_game_full, args=(vday, vserverID,
                            row['slave_game_db_host'], row['slave_game_db_port'], row['slave_game_db_user'],
                            row['slave_game_db_password'], row['slave_game_db_name'],
                            subRow['TABLE_NAME'], betweenDay, columnList, createFolderName, dbType)
                        )


                        #if(len(backupResults)) == 1:
                        #    break



                    elif dbType == 'LOG' and backupYes:
                        betweenDay = 1

                        if exceptionList != "":
                            columnList = get_select_column(row['slave_log_db_host'], row['slave_log_db_port'],
                                                           row['slave_log_db_user'],
                                                           row['slave_log_db_password'], row['slave_log_db_name'],
                                                           subRow['TABLE_NAME'], exceptionList)
                        
                        pool.apply_async(get_work, args=(vday, vserverID,
                                                           row['slave_log_db_host'], row['slave_log_db_port'],
                                                           row['slave_log_db_user'],
                                                           row['slave_log_db_password'], row['slave_log_db_name'],
                                                           subRow['TABLE_NAME'], betweenDay, columnList, createFolderName, dbType, data_hour)
                                         )


                    elif dbType == 'BIG' and backupYes:
                        betweenDay = 1

                        pool.apply_async(get_biglog, args=(vday, vserverID,
                                                           row['slave_log_db_host'], row['slave_log_db_port'],
                                                           row['slave_log_db_user'],
                                                           row['slave_log_db_password'], row['slave_log_db_name'],
                                                           subRow['TABLE_NAME'], betweenDay, columnList, createFolderName, dbType)
                                         )



                    elif dbType == 'LOGWEEK' and backupYes:
                        betweenDay = 201
                        # vday 가 속한 주차
                        pool.apply_async(get_work, args=(vday, vserverID,
                                                           row['slave_log_db_host'], row['slave_log_db_port'],
                                                           row['slave_log_db_user'],
                                                           row['slave_log_db_password'], row['slave_log_db_name'],
                                                           subRow['TABLE_NAME'], betweenDay, columnList, createFolderName, dbType)
                                    )



                    elif dbType == 'LOGMONTH' and backupYes:
                        betweenDay = 301
                        # vday 가 속한 월
                        pool.apply_async(get_work, args=(vday, vserverID,
                                                           row['slave_log_db_host'], row['slave_log_db_port'],
                                                           row['slave_log_db_user'],
                                                           row['slave_log_db_password'], row['slave_log_db_name'],
                                                           subRow['TABLE_NAME'], betweenDay, columnList, createFolderName, dbType)
                                    )



                    elif dbType == 'LOGQUARTER' and backupYes:
                        betweenDay = 401
                        # vday 가 속한 분기
                        pool.apply_async(get_work, args=(vday, vserverID,
                                                           row['slave_log_db_host'], row['slave_log_db_port'],
                                                           row['slave_log_db_user'],
                                                           row['slave_log_db_password'], row['slave_log_db_name'],
                                                           subRow['TABLE_NAME'], betweenDay, columnList, createFolderName, dbType)
                                    )



                    elif dbType == 'LOGYEAR' and backupYes:
                        betweenDay = 501
                        # vday 가 속한 년
                        pool.apply_async(get_work, args=(vday, vserverID,
                                                           row['slave_log_db_host'], row['slave_log_db_port'],
                                                           row['slave_log_db_user'],
                                                           row['slave_log_db_password'], row['slave_log_db_name'],
                                                           subRow['TABLE_NAME'], betweenDay, columnList, createFolderName, dbType)
                                    )



                    elif dbType == 'STAT' and backupYes:
                        betweenDay = 101

                        if exceptionList != "":
                            columnList = get_select_column(row['slave_stat_db_host'], row['slave_stat_db_port'],
                                                           row['slave_stat_db_user'],
                                                           row['slave_stat_db_password'], row['slave_stat_db_name'],
                                                           subRow['TABLE_NAME'], exceptionList)

                        pool.apply_async(get_work, args=(vday, vserverID,
                                                           row['slave_stat_db_host'], row['slave_stat_db_port'],
                                                           row['slave_stat_db_user'],
                                                           row['slave_stat_db_password'], row['slave_stat_db_name'],
                                                           subRow['TABLE_NAME'], betweenDay, columnList, createFolderName, dbType)
                                    )



                    elif dbType == 'OPENDATA' and backupYes:
                        #n = datetime.datetime.strptime(vday, '%Y-%m-%d')

                        #n = n + datetime.timedelta(days=-1)

                        #preDay = n.strftime('%Y-%m-%d')

                        pool.apply_async(get_opendata, args=(vday, game_code, vserverID, subRow['TABLE_NAME'], columnList, createFolderName))
                        #pool.apply_async(get_opendata, args=(vday, game_code, vserverID, "v_unvs_fd_bot_mst", columnList, createFolderName))
                        #v_unvs_fd_bot_mst > 접근 불가
                        #v_unvs_fd_users_sale_acc > 접근 불가

                    elif dbType == 'CRASH' and backupYes:
                        #n = datetime.datetime.strptime(vday, '%Y-%m-%d')

                        #n = n + datetime.timedelta(days=-1)

                        #preDay = n.strftime('%Y-%m-%d')

                        pool.apply_async(get_crash, args=(vday, vserverID,subRow['TABLE_NAME'], columnList, createFolderName))

                    elif dbType == 'ASSET' and backupYes:
                        betweenDay = 0
                        meta_info = lobby_conn.getMetaLocaleInfos(location, region)[0]

                        if exceptionList != "":
                            columnList = get_select_column(meta_info['ip'], meta_info['port'],
                                                           meta_info['user'],
                                                           meta_info['password'], meta_info['database'],
                                                           subRow['TABLE_NAME'], exceptionList)

                        
                        pool.apply_async(get_work, args=(vday, vserverID,
                                                           meta_info['ip'], meta_info['port'],
                                                           meta_info['user'], meta_info['password'],
                                                           meta_info['database'],
                                                           subRow['TABLE_NAME'], betweenDay, columnList, createFolderName, dbType)
                                    )


            else:
                anal_infos = ct.getAnalyticsLocaleInfos(location)
                betweenDay = 0

                if table_name is not None:
                    for row in backupResults:
                        if row["table_name"] == table_name:
                            backupResults = []
                            backupResults.append(row)
                            break;

                for row in backupResults:
                    backup_table = row["table_name"]
                    columnList = row["column_list"]

                    pool.apply_async(get_work,
                                args=(vday,
                                      vserverID,
                                      anal_infos[0]["ip"],
                                      anal_infos[0]["port"],
                                      anal_infos[0]["user"],
                                      anal_infos[0]["password"],
                                      anal_infos[0]["database"],
                                      backup_table,
                                      betweenDay,
                                      columnList,
                                      createFolderName,
                                      dbType)
                                )

            pool.close()
            pool.join()

            #print(listTable)

    #print(localeGameDB)

if __name__ == '__main__':


    vserverID = sys.argv[1]
    dbType = sys.argv[2].replace("'","")
    gamecode = sys.argv[3].replace("'","")
    location = sys.argv[4].replace("'","")
    region = sys.argv[5].replace("'","")



    print(len(sys.argv))
    if len(sys.argv) >= 7:
        vday = sys.argv[6]
    else:
        n = datetime.datetime.now()
        n = n + datetime.timedelta(days=-1)
        vday = n.strftime('%Y-%m-%d')

    table_name = None
    if len(sys.argv) >= 8:
        table_name = sys.argv[7]

    if len(sys.argv) >= 9:
        data_hour = sys.argv[8]

    if str(data_hour) == '24':

        sum_df = pd.DataFrame()

        for number in range(24):
            main_function(vserverID, dbType, gamecode, location, region, vday, table_name, number)

        createFolderName = os.getcwd() + "/" + location + "/" + region + "/" + vserverID + "/" + str(vday)


        for number in range(24):
            part_df = pd.read_csv(createFolderName + "/" + table_name + '_' + str(number) + '.csv')
            sum_df = pd.concat([sum_df,part_df])
        sum_df.to_csv(createFolderName + "/" + table_name + '.csv')

    else:
        main_function(vserverID, dbType, gamecode, location, region, vday, table_name, data_hour)


    if systemStat <= 0:
        sys.exit(-1)


