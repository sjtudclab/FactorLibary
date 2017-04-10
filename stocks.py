# pylint: disable=I0011,C0103,C0326,C0301, W0401,W0614
from WindPy import *
from datetime import datetime
import time
from cassandra.cluster import Cluster

### 获取基本股票信息
## 可交易日/周/月
## 所有股票名/IPO日期/可交易状态(待修正，每次查询A股都要判断是否处于可交易状态，因此严格来说每次数据获取和导出时都要加判定)

def updateAStocks(extraIndex = []):
    # 启动Wind API
    w.start()
    #取全部 A 股股票代码、名称信息(不写field，默认为wind_code & sec_name & date)
    stocks = w.wset("SectorConstituent",u"sector=全部A股;field=wind_code,sec_name")
    data = stocks.Data
    size = len(data[0]) 

    indexInfo = []
    for index in extraIndex:
        d = w.wsd(index, "windcode,sec_name", "2017-04-06", "2017-04-06", "Period=Y").Data
        indexInfo.append((d[0][0], d[1][0]))
    print ("index info: "+indexInfo)
    print("stock number: ", size)

    cluster = Cluster(['192.168.1.111'])
    session = cluster.connect('factors') #connect to the keyspace 'factors'
    preparedStmt = session.prepare('''INSERT INTO stock_info(stock, sec_name) VALUES (?,?)''')
    for i in range(size):
        session.execute(preparedStmt,(data[0][i],data[1][i]))
    print ("Updating stocks of A share complete!")
    for i in range(len(indexInfo)):
        session.execute(preparedStmt,(indexInfo[i][0],indexInfo[i][1]))
    print ("Updating Stock Index complete!")

#取沪深 300 指数中股票代码和权重
#stocks = w.wset("IndexConstituent","date=20130608;windcode=000300.SH;field=wind_code,i_weight")

# # Testing
# print ("testing: select * from transaction_time where type='month' and time > '2016-03-02' ALLOW FILTERING;")
# rows = session.execute("select * from transaction_time where type='month' and time > '2016-03-02' ALLOW FILTERING;")
# for row in rows:
#     print(row.time)

#从周因子表中获取股票600000.SH在2017-03-02至今的所有因子
# rows = session.execute("select * from factors_week where stock='600000.SH' and time > '2017-03-02' ALLOW FILTERING;")
# for row in rows:
#     print(row.stock,row.factor,row.time,row.value)

## Testing： get all stocks 
# rows = session.execute('''select stock from stock_info''')
# stocks = []
# for row in rows:
#     stocks.append(row[0])
# print (stocks)

## insert transaction day
def updateTransactionTime(startTime, endTime = datetime.today(),TYPE='D'):
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),"Updating Transaction Time in TYPE: ", TYPE)
    # 启动Wind API
    w.start()
    times = w.tdays(startTime, endTime, "Period="+TYPE).Times
    timeList = []
    for i in range(len(times)):
        row = str(times[i])
        row = row[:row.find(' ')]
        timeList.append(row)
    print(timeList)

    #cassandra connect  to the keyspace 'factors'
    cluster = Cluster(['192.168.1.111'])
    session = cluster.connect('factors')
    preparedStmt = session.prepare('''INSERT INTO transaction_time(type, time) VALUES (?,?)''')
    for date in timeList:
        session.execute(preparedStmt, (TYPE, date))
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())," Updating Complete!")
#########################################################
## Updating Available A share Stock & transaction_time ##
#########################################################
# transaction day
#updateTransactionTime('2009-01-01')
updateTransactionTime('2009-01-01', TYPE='M')
# update stocks
#updateAStocks(extraIndex=["000001.SH","399001.SZ",'399006.SZ','000300.SH','000016.SH','000905.SH'])