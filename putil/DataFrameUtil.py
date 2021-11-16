import json
import pandas as pd
#import modin.pandas as pd
import numpy as np
import math
import re



class DataFrameUtil:
    def columns_data_type(headers):
        """
            데이터 프레임 생성할떄 컬럼 정보에 따른 데이터 타입 지정
        """
        data_types = {}
        for column in headers:
            if 'p_month'== column:
                data_types[column] = 'int8'
            elif 'world_info_id'== column:
                data_types[column] = 'int16'
            elif 'tr_id'== column:
                data_types[column] = 'int32'
            elif 'character_level'== column:
                data_types[column] = 'uint16'
            elif 'level'== column:
                data_types[column] = 'uint16'
            elif 'enchant_level'== column:
                data_types[column] = 'uint16'
            elif 'class_info_id'== column:
                data_types[column] = 'uint8'
            elif 'combat_power'== column:
                data_types[column] = 'uint32'
            elif 'os_type'== column:
                data_types[column] = 'uint8'
            elif 'user_type'== column:
                data_types[column] = 'uint8'
            elif 'diff_days'== column:
                data_types[column] = 'float32'
            elif 'diff_days'== column:
                data_types[column] = 'float32'
            elif 'info_id'== column:
                data_types[column] = 'uint32'
            elif 'gem_info_id1'== column:
                data_types[column] = 'uint32'
            elif 'gem_info_id2'== column:
                data_types[column] = 'uint32'
            elif 'gem_info_id3'== column:
                data_types[column] = 'uint32'
            elif 'gem_type1'== column:
                data_types[column] = 'uint8'
            elif 'gem_type2'== column:
                data_types[column] = 'uint8'
            elif 'gem_type3'== column:
                data_types[column] = 'uint8'
            elif 'is_legendary'== column:
                data_types[column] = 'uint8'
            elif 'is_legendary'== column:
                data_types[column] = 'uint8'
            elif 'asset_type'== column:
                data_types[column] = 'uint8'

        return data_types


    def columns_type(column, type):
        """
            데이터 프레임 생성할떄 컬럼 정보에 따른 데이터 타입 지정
        """
        if type != np.object:
            return None        
        if "param" in column :
            return 'int64'        
        elif "npc_info_id" == column:
            return 'uint32'
        elif "_id" in column:
            return 'int64'
        return None

    def table_loding_columns(data_name, csv_headers, etl_info, j_info, a_info):
        '''
            데이터 이름(테이블, 로그) 에 따라 로딩해야하는 컬럼을 지정한다.
        '''
        get_asset_column=[]
        group_columns = DataFrameUtil.group_column(j_info['group'])
        agg_columns = DataFrameUtil.get_agg_column(j_info['agg'])
        get_join_column = DataFrameUtil.get_join_column(j_info['join_column'])
        where_column = DataFrameUtil.get_where_column(etl_info['wherestr'])
        if a_info:
            get_asset_column = DataFrameUtil.get_join_column(a_info['join_column'])
        allColumns = get_asset_column + group_columns + agg_columns + get_join_column + where_column
        result_column = []
        for column in allColumns:
            if '_x' in column:
                column = column[:-2]
            if '_y' in column:
                column = column[:-2]

            if column in csv_headers:
                if column not in result_column:
                    result_column.append(column)
        return result_column

    def table_where(df, where_text):
        if not where_text:
            return df
        where_columns = DataFrameUtil.get_where_column(where_text)
        where_list = where_text.split('and')
        columns = df.columns
        where =[]
        for w_column in where_columns:
            for where_text in where_list : 
                if w_column in where_text and w_column in columns :
                    where.append(where_text)
        if where:
            select_where = ' and '.join(where)
            return df.query(select_where)
        else:
            return df

    
    
    def get_where_column(where_text):
        result_list = []
        if where_text:
            text_list = where_text.split(' ')
            for text in text_list:
                text = text.strip()
                if '>' in text or '<' in text or '=' in text or 'in' == text  :
                    continue
                elif any(chr.isdigit() for chr in text):
                    continue
                else :
                    result_list.append(text)

            return result_list
        else:
            return []
    def get_agg_column(agg_text):
        agg_dict = json.loads(agg_text.strip())
        result_list = []
        for column, list  in agg_dict.items():
            result_list.append(column)
        return result_list

    def get_join_column(join_text):
        if join_text:
            return_list = []
            join_info = json.loads(join_text)
            for key, columns in join_info.items():
                if type(columns) == str:
                    return_list.append(columns)
                elif type(columns) == list:
                    for column in columns:
                        return_list.append(column)
            return return_list
        else:
            return []



    def join_info(data_type, etl_info):
        """
            etl 정보에서 필요한 정볼르 가지고온다.
            파일정보, 
        """
        j_info = {}
        if data_type.lower() == 'game':
            j_info['join_table'] = etl_info['join_table']
            j_info['join_column'] = etl_info['join_column']
            j_info['group'] = etl_info['groupstr']
            j_info['agg'] = etl_info['countstr']
            return j_info
        elif data_type.lower() == 'open':
            j_info['join_table'] = etl_info['datamart_join_table']
            j_info['join_column'] = etl_info['datamart_join_column']
            j_info['group'] = etl_info['datamart_groupstr']
            j_info['agg'] = etl_info['datamart_countstr']
            return j_info
        elif data_type.lower() == 'anomaly':
            j_info['join_table'] = etl_info['anomaly_join_table']
            j_info['join_column'] = etl_info['anomaly_join_column']
            j_info['group'] = etl_info['[anomaly_groupstr']
            j_info['agg'] = etl_info['anomaly_countstr']
            return j_info
        return None

    def asset_info(etl_info):
        """
            etl 에셋 정보를 가지고 온다.
        """
        if not etl_info['asset_table']: 
            return None
        a_info={}
        a_info['join_table'] = etl_info['asset_table']
        a_info['join_column'] = etl_info['asset_table_join_column']
        return a_info

    def join_left(join_info):
        join_dic = json.loads(join_info)
        if "on" in join_dic.keys():
            return join_dic["on"]
        elif "left" in join_dic.keys():
            return join_dic["left"]
        else: 
            raise Exception('join_left 조건을 찾을 수 없습니다.')

    def join_right(join_info):
        join_dic = json.loads(join_info)
        if "on" in join_dic.keys():
            return join_dic["on"]
        elif "right" in join_dic.keys():
            return join_dic["right"]
        else: 
            raise Exception('join_right 조건을 찾을 수 없습니다.')

    def merge_dataframe(df, join_df, join_info):
        """
         데이터를 머지한다.
        """
        if join_info['join_column'] == None :
            return df
        left_column = DataFrameUtil.join_left(join_info['join_column'])
        right_column = DataFrameUtil.join_right(join_info['join_column'])
        return pd.merge(df, join_df, how="inner",left_on=left_column, right_on=right_column)


    def where_dataframe(df, where):
        """
            DF 필요한 조건만 가지고온다.
        """
        if where:
            return df.query(where)
        else:
            return df

    def group_column(group):
        '''
           ㅑ 그룹 정보를 가지고온다.
        '''
        if not group :
            return []
        colums = []
        tempList = group.replace(" ","").strip().split(',')
        for name in tempList:
            if 'ceil10000' in name:
                colums.append(name[10:-1])
            elif 'ceil5' in name:
                colums.append(name[6:-1])
            else:
                colums.append(name)
        return colums
    def make_group_column(group):
        '''
            그룹 정보를 가지고온다.
        '''
        if not group :
            return []
        colums = []
        tempList = group.replace(" ","").strip().split(',')
        for name in tempList:
            if 'ceil10000' in name:
                colums.append(name[10:-1]+'10000')
            elif 'ceil5' in name:
                colums.append(name[6:-1]+'5')
            else:
                colums.append(name)
        return colums
    def create_ceil_column(df, group):
        if not group :
            return df
        tempList = group.strip().split(',')
        for name in tempList:
            if 'ceil10000' in name:
                column_name = name[10:-1]
                new_column_name = name[10:-1]+'10000'
                df[new_column_name] = list(map(lambda x : math.ceil(x/10000) * 10000, df[column_name]))
            elif 'ceil5' in name:
                column_name = name[6:-1]
                new_column_name = name[6:-1]+'5'
                df[new_column_name] = list(map(lambda x : math.ceil(x/5) * 5, df[column_name]))
        #print(df)
        return df

    def aggregate_dict(agg_text):
        agg_dict = json.loads(agg_text)
        for column, aggs in agg_dict.items():
            if type(aggs) is not  list:
                gg_dict[column] = list(aggs)
            # 집계 함수 변환
            for index, value in enumerate(aggs):
                value = str(value).strip()
                if "percentile" in value:
                    aggs[index] = DataFrameUtil.percentile(int(re.findall('\d+', value)[0]))
        return agg_dict


    def percentile(n):
        def percentile_(x):
            return np.percentile(x, n)
        percentile_.__name__ = 'percentile_%s' % n
        return percentile_
    def group_dataframe(df, j_info):
        '''
        데이터를 집계 처리 한다
        '''
        group_columns = DataFrameUtil.make_group_column(j_info['group'])
        agg_dict = DataFrameUtil.aggregate_dict(j_info['agg'].strip())
        
        if len(group_columns) > 0:            
            df = df.groupby(group_columns).agg(agg_dict)

            if type(df.columns) is pd.core.indexes.base.Index:
                list_columns = []
                for key, value in agg_dict.items():
                    list_columns.append(key + "_" + value)
                df.columns = list_columns
            else:
                df.columns = ["_".join(x) for x in df.columns.ravel()]
        else:
            df = df.agg(agg_dict)
        df = df.reset_index()

        return df
    def rename_column(df):
        if "tr_id_x" in df.columns:
            df.rename(columns={"tr_id_x": "tr_id"}, inplace=True)

        if "tr_id_y" in df.columns:
            df.rename(columns={"tr_id_y": "tr_id"}, inplace=True)

        if "user_id_x_nunique" in df.columns:
            df.rename(columns={"user_id_x_nunique": "user_id_nunique"}, inplace=True)

        if "user_id_y_nunique" in df.columns:
            df.rename(columns={"user_id_y_nunique": "user_id_nunique"}, inplace=True)

        if "user_id_x_count" in df.columns:
            df.rename(columns={"user_id_x_count": "user_id_count"}, inplace=True)
        return df




