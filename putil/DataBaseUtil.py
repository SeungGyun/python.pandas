import pymysql
import zlib
class DataBaseUtil:
    def save_df(args,server_id, etl_info, db_info, j_info, df):
        """
            집계된 데이터를 저장한다.
        """
        try:
            conn = DataBaseUtil.analytics_conn(db_info)
            cursor=conn.cursor()
            delete_sql = DataBaseUtil.delete_sql(args, server_id, etl_info)
            cursor.execute(delete_sql)            
            insert_sql = DataBaseUtil.insert_sql(args, server_id, etl_info)
            #text = df.to_json(orient="split")
            #cmp_text = zlib.compress(text.encode('utf-8'))
            #uncmpstr = zlib.decompress(cmp_text)
            #fmt = '{:>8}: (length {}) {!r}'
            #print(fmt.format('teststr', len(text), text))
            #print(fmt.format('cmpstr', len(cmp_text), cmp_text))
            #print(fmt.format('uncmpstr', len(uncmpstr), uncmpstr))
            cursor.execute(insert_sql, df.to_json(orient="split"))
            conn.commit()
        except Exception as ex:
            print(f"DataBaseUtil- save_df {etl_info['etl_filename']}- {j_info['join_table']} - {etl_info['asset_table']}, error message : {ex}")
            raise Exception(f"DataBaseUtil - save_df :  {etl_info['etl_filename']}- {j_info['join_table']} - {etl_info['asset_table']}, error message : {ex}")
        finally:
            if conn :
                conn.close()

    def insert_sql(args, server_id, etl_info):
        '''
            인서트 쿼리
        '''
        data_type = args.datatype.lower()
        istemp = args.istemp;
        return f"""
                INSERT INTO {istemp}{etl_info['stat_table_name']}_{data_type}   
                    (gamecode, location, region, today_date , server_id, table_id, stat_type, stat_data)
                VALUES 
                    ('{args.gamecode}','{args.location}','{args.region}','{args.date}', '{server_id}', '{etl_info['table_id']}', '{etl_info['stat_type']}', %s)
                """

    def delete_sql(args, server_id, etl_info):
        '''
            삭제 쿼리
        '''
        data_type = args.datatype.lower()
        istemp = args.istemp;
        return f"""
                    DELETE 
                        FROM {istemp}{etl_info['stat_table_name']}_{data_type} 
                    WHERE 
                        today_date =  '{args.date}' 
                    AND server_id =  '{server_id}' 
                    AND table_id =  '{etl_info['table_id']}' 
                    AND stat_type =  '{etl_info['stat_type']}'
                    AND region =  '{args.region}'
                    AND gamecode =  '{args.gamecode}'
                    AND location =  '{args.location}'
                """

    def analytics_conn(db_info):
        return pymysql.connect(
            host=db_info['ip'],
            port=db_info['port'],
            user=db_info['user'],
            password=db_info['password'],
            db=db_info['database']
        )