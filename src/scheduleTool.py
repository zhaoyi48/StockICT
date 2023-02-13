import sqlalchemy
import akshare as ak
import configparser
import schedule
import pandas
from datetime import datetime
import os
import time
# from zoneinfo import ZoneInfo


config_path=os.environ.get('CONFIG_PATH','/config/config.ini')
print(config_path)

config = configparser.ConfigParser()
config.read(config_path)

database_connection = None
is_trade_date=False

def init():
    global database_connection
    database_username = config['MYSQL']['database_username']#'root'
    database_password = config['MYSQL']['database_password']#'password'
    database_ip       = config['MYSQL']['database_ip']#'192.168.31.169'
    database_name     = config['MYSQL']['database_name']#'stock'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password, 
                                                          database_ip, database_name))
def getNowDateTime(format_str='%Y-%m-%d'):
    return datetime.now().strftime(format_str) #ZoneInfo('Asia/Shanghai')

def importToMysql(df, tablename, index=None, if_exists='append',add_dateTime=True):
    import_df=df.fillna(-1)
    if add_dateTime is True:
        import_df['DateTime']=getNowDateTime('%Y-%m-%d %H:%M:%S')
    col_dict={}
    df_coldict=import_df.dtypes.to_dict()
    for col_name, typ in df_coldict.items():
        if typ==object:
            col_dict[col_name]=sqlalchemy.types.VARCHAR(length=255)
        if typ=='float64':
            col_dict[col_name]=sqlalchemy.types.Numeric(18,2)
        if typ=='int64':
            col_dict[col_name]=sqlalchemy.types.INTEGER()
#     if database_connection is None:
#         init()
    if index is None:
        import_df=import_df.set_index(import_df.columns[0])
    else:
        import_df=import_df.set_index(index)
    import_df.to_sql(con=database_connection, name=tablename, if_exists=if_exists,
                                        dtype=col_dict)
    
def getData(method_name):
    df=getattr(ak,method_name)
    return df()

def importData(method_name,tablename,if_exists):
        df_data=getData(method_name)
        if method_name == 'stock_sse_summary':
            df_data=df_data.T.reset_index().rename(columns=df_data.T.reset_index().iloc[0])
            df_data.drop(df_data.index[0], inplace = True)
            importToMysql(df_data,tablename,if_exists=if_exists,add_dateTime=True)
        else:
            importToMysql(df_data,tablename,if_exists=if_exists)
            
def startMonitor(parm,task_name,tablename,if_exists,interval_type):
    if interval_type == 'minutes':
        schedule.every(parm).minutes.do(importData,task_name,tablename,if_exists).tag('ak-tasks',task_name)
    if interval_type == 'hours':
        schedule.every(parm).hours.do(importData,task_name,tablename,if_exists).tag('ak-tasks',task_name)
    if interval_type == 'seconds':
        schedule.every(parm).seconds.do(importData,task_name,tablename,if_exists).tag('ak-tasks',task_name)
    
def cancelMonitor(task_name):
    schedule.clear(task_name)
    
def buildSchedule():
    for key in config['Time.point']:
        print(key)
        array=key.split('.')
        if len(array) == 4:
            parm=config['Time.point'][key]
            if array[3] == 'minutes':
                schedule.every().minute.at(parm).do(importData,array[0],array[1],array[2]).tag('ak-tasks')
            if array[3] == 'day':
                schedule.every().day.at(parm).do(importData,array[0],array[1],array[2]).tag('ak-tasks')
            if array[3] == 'hour':
                schedule.every().hour.at(parm).do(importData,array[0],array[1],array[2]).tag('ak-tasks')
        else:
            print('Error:config'+key)
    for key in config['Time.interval']:
        print(key)
        array=key.split('.')
        if len(array) == 4:
            parm=config['Time.interval'][key]
            schedule.every().day.at('09:30').do(startMonitor,parm,array[0],array[1],array[2],array[3]).tag('ak-tasks')
            schedule.every().day.at('11:30').do(cancelMonitor,array[0]).tag('ak-tasks')
            schedule.every().day.at('13:00').do(startMonitor,parm,array[0],array[1],array[2],array[3]).tag('ak-tasks')
            schedule.every().day.at('15:00').do(cancelMonitor,array[0]).tag('ak-tasks')
        else:
            print('Error:config'+key)
            
def check_trade_date():
    tool_trade_date_hist_sina_df=getData('tool_trade_date_hist_sina')
    tool_trade_date_hist_sina_df['trade_date'] = pandas.to_datetime(tool_trade_date_hist_sina_df['trade_date'],format="%Y-%m-%d")
    trade_date_now=getNowDateTime()
    global is_trade_date
    is_trade_date=tool_trade_date_hist_sina_df['trade_date'].eq(trade_date_now).any()
    schedule.clear('ak-tasks')
    if is_trade_date==True:
        buildSchedule()

print('Init database')
init()
        
print('Check Trade day')            
check_trade_date()
if is_trade_date==True:
    print('{} is Trade day'.format(getNowDateTime('%Y-%m-%d')))
else:
    print('{} is not Trade day'.format(getNowDateTime('%Y-%m-%d')))
    
print('Start Monitor')
schedule.every().day.at("00:00").do(check_trade_date).tag('check-tasks')
while True:
    schedule.run_pending()
    time.sleep(1)