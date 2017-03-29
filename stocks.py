from WindPy import *
from datetime import datetime

### 获取基本股票信息
## 可交易日/周/月
## 所有股票名/IPO日期/可交易状态

# 启动Wind API
w.start()
# 获取可交易日
startTime = '2009-01-01'
endTime = datetime.today() # 可缺省，默认今天

times = w.tdays(startTime, endTime, "Period=M").Times

timeList = []
for i in range(len(times)):
    row = str(times[i])
    row = row[:row.find(' ')]
    timeList.append(row)
print(timeList)

#取沪深 300 指数中股票代码和权重
#stocks = w.wset("IndexConstituent","date=20130608;windcode=000300.SH;field=wind_code,i_weight")
#取全部 A 股股票代码、名称信息(不写field，默认为wind_code & sec_name & date)
#stocks = w.wset("SectorConstituent",u"sector=全部A股")
#stocks = w.wset("SectorConstituent",u"sector=全部A股;field=wind_code,sec_name")
#data = stocks.Data
##3147 stocks until now
#print("length: ", len(data[0]), data)

#cassandra connection
from cassandra.cluster import Cluster

cluster = Cluster(['192.168.1.111'])
session = cluster.connect('factors') #connect to the keyspace 'factors'
preparedStmt = session.prepare('''INSERT INTO transaction_time(type, time) VALUES (?,?)''')

for date in timeList:
    session.execute(preparedStmt, ('month', date))

# Testing
print ("testing: select * from transaction_time where type='month' and time > '2016-03-02' ALLOW FILTERING;")
rows = session.execute("select * from transaction_time where type='month' and time > '2016-03-02' ALLOW FILTERING;")
for row in rows:
    print(row.time)

#从周因子表中获取股票600000.SH在2017-03-02至今的所有因子
# rows = session.execute("select * from factors_week where stock='600000.SH' and time > '2017-03-02' ALLOW FILTERING;")

# for row in rows:
#     print(row.stock,row.factor,row.time,row.value)


