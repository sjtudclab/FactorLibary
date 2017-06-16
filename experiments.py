''' Retrieve Daily Factors '''
# pylint: disable=I0011,C0103,C0326,C0301, W0401,W0614

from datetime import datetime
from WindPy import *
from cassandra.cluster import Cluster
from cassandra.util import Date
import time
import datetime
import math
import numpy as np
import csv
import os

def retrieveSingleFactor(factor, startTime, endTime, table='factors_day', stocks=[],option = "Period=D;Fill=Previous;PriceAdj=B"):
    # 启动Wind API
    w.start()

    # 获取交日 序列
    times = w.tdays(startTime, endTime, option).Times
    timeList = []
    for i in range(len(times)):
        row = str(times[i])
        row = row[:row.find(' ')]
        timeList.append(row)
    print(timeList)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), ' TOTAL DAYS: ', len(timeList), 'Pulling Started...\n --------------------------')

    # 获取日数据收盘价（向后复权）
    # wind一次只能拉一只股票,数据写入Cassandra
    cluster = Cluster(['192.168.1.111'])
    session = cluster.connect('experiment') # connect to database
    sql = "INSERT INTO "+table
    preparedStmt = session.prepare(sql + "(stock, factor, time, value) VALUES (?,?,?,?)")

    # go through all  Stock Index
    
    for stock in stocks:
        cnt = 0
        wsd_data = w.wsd(stock, factor, startTime, endTime, option).Data
        for j in range(len(timeList)):
            #print (stock,factor,timeList[j],str(wsd_data[0][j]))
            try:
                value = wsd_data[0][j]
                if value is not None:
                    value = float(value)
                else:
                    value = 0
            except (ValueError, TypeError, KeyError) as e:
                        value = 0
                        print ("--Log ValueError in ", stock,"\t",str(timeList[j]),"\t",str(value))
                        print (e)
                        print ("--------------------------------------------------------------------------")
            session.execute_async(preparedStmt, (stock, factor,timeList[j], value))
            cnt += 1
            if cnt % 1000 == 0:
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) ,'------ Dump NO.%d end at stock %s \n' % (cnt, stock))
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '---------------- Persistion finished!\n')

    #result testing
    print("---------- Inserstion Testing: ")
    rows = session.execute("select * from "+table+" where stock='"+stocks[0]+"' and factor = '"+factor+"' and time >= '2017-03-02'")
    for row in rows:
        print(row.stock,row.factor,row.time,row.value)
    # close connection with cassandra
    cluster.shutdown()

def retrieveMultipleFactors(factors, startTime, endTime, table='factors_day', stocks=[],option = "Period=D;Fill=Previous;PriceAdj=B"):
    # 启动Wind API
    w.start()

    # 获取交日 序列
    times = w.tdays(startTime, endTime, option).Times
    timeList = []
    for i in range(len(times)):
        row = str(times[i])
        row = row[:row.find(' ')]
        timeList.append(row)
    print(timeList)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), ' TOTAL DAYS: ', len(timeList), 'Pulling Started...\n --------------------------')

    # 获取日数据收盘价（向后复权）
    # wind一次只能拉一只股票,数据写入Cassandra
    cluster = Cluster(['192.168.1.111'])
    session = cluster.connect('experiment') # connect to database
    sql = "INSERT INTO "+table
    preparedStmt = session.prepare(sql + "(stock, factor, time, value) VALUES (?,?,?,?)")

    # go through all  Stock Index
    for stock in stocks:
        cnt = 0
        wsd_data = w.wsd(stock, factors, startTime, endTime, option).Data
        #print(wsd_data)
        for i in range(len(factors)):
            factor = factors[i]
            for j in range(len(timeList)):
                #print (stock,factor,timeList[j],str(wsd_data[0][j]))
                try:
                    value = wsd_data[i][j]
                    #print ("--Log  in ", stock,"\t",factor,"\t",str(timeList[j]),"\t",str(value))
                    if value is not None:
                        value = float(value)
                    else:
                        value = 0
                except (ValueError, TypeError, KeyError) as e:
                            value = 0
                            print ("--Log ValueError in ", stock,"\t",str(timeList[j]),"\t",str(value))
                            print (e)
                            print ("--------------------------------------------------------------------------")
                session.execute_async(preparedStmt, (stock, factor, timeList[j], value))
                cnt += 1
                if cnt % 1000 == 0:
                    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) ,'------ Dump NO.%d end at stock %s \n' % (cnt, stock))
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '---------------- Persistion finished!\n')

    #result testing
    print("---------- Inserstion Testing: ")
    rows = session.execute("select * from "+table+" where stock='"+stocks[0]+"' and factor = '"+factor+"' and time >= '2017-03-02'")
    for row in rows:
        print(row.stock,row.factor,row.time,row.value)
    # close connection with cassandra
    cluster.shutdown()

def updateTransactionTime(startTime, endTime = datetime.datetime.today(),TYPE='D'):
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

    #cassandra connect  to the keyspace 'experiment'
    cluster = Cluster(['192.168.1.111'])
    session = cluster.connect('experiment')
    preparedStmt = session.prepare('''INSERT INTO transaction_time(type, time) VALUES (?,?)''')
    for date in timeList:
        session.execute(preparedStmt, (TYPE, date))
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())," Updating Complete!")

def calcMeanPrice(startTime, endTime, stocks, factor = 'close'):
    #cassandra connect  to the keyspace 'experiment'
    cluster = Cluster(['192.168.1.111'])
    session = cluster.connect('experiment')
    # 所有周末时间
    rows = session.execute("select time from transaction_time WHERE type='W' and time >= %s and time<= %s ",(startTime,endTime))
    days = []
    for row in rows:
        days.append(row[0])

    #(end_of_last_week, end_of_this_week]
    selectPreparedStmt = session.prepare("select value from factors_day where stock = ? and factor = ? and time > ? and time <= ? ALLOW FILTERING")
    insertPreparedStmt = session.prepare("INSERT INTO factors_week (stock, factor, time, value) VALUES (?, ?, ?, ?)")

    for stock in stocks:
        prevDay = startTime
        for day in days:
            price_sum = 0.0
            cnt = 0
            rows = session.execute(selectPreparedStmt, (stock, factor, prevDay, day))
            for row in rows:
                if row.value is not None:
                    value  = row.value
                    price_sum += value
                    cnt += 1
            # print(prevDay, " -- ", day, stock, factor, day, price_sum/cnt)
            prevDay = day
            # insert into db
            if cnt != 0:
                session.execute(insertPreparedStmt, (stock, factor, day, price_sum/cnt))
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '---------------- Calculate %s %s complete!'%(stock, str(factor)))

''' Generate Factors File '''
def generateData(fileName, startTime, endTime, stocks, table = "factors_day", TYPE='D'):
    if startTime > endTime:
        return

    cluster = Cluster(['192.168.1.111'])
    session = cluster.connect('experiment') #connect to the keyspace 'factors'

    print (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "Retrieving data: ", len(stocks))
    #time list
    rows = session.execute('''
        select * from transaction_time 
        where type= %s and time > %s and time < %s ALLOW FILTERING;''', [TYPE,startTime, endTime])

    SQL = "SELECT value FROM "+table+" WHERE stock = ? AND factor = 'close' and time >= '" + str(startTime) +"' and time <= '" + str(endTime)+"'"
    preparedStmt = session.prepare(SQL)

    dateList = []
    for row in rows:
        dateList.append(row.time)
    # 拉取数据,一次拉一只股票
    dataList = []
    for stock in stocks:
        rows = session.execute(preparedStmt,(stock,))
        data = []
        for row in rows:
            data.append(row[0])
        dataList.append(data)
    cluster.shutdown()

    # 数据写入文件中
    f = open(fileName, "w")
    f.write('time')
    for stock in stocks:
        f.write(','+stock)
    f.write('\n')
    colNum = len(stocks)
    rowNum = len(dateList)
    for i in range(rowNum):
        f.write(str(dateList[i]))
        for s in range(colNum):
            try:
                data = dataList[s][i]
                if math.isnan(data):
                    data = 0  # default value
                f.write(',' + str(data))
            except IndexError:
                print ("End of reading and writing daily close data...")
                f.close()
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), 'Writing to ', fileName, ' complete!')
                return
            #print (timeList[i],stocks[s],dataList[s][0][i])
        f.write('\n')
    f.close()
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), 'Writing to ',fileName,' complete!')

if __name__ == '__main__':
    ## add Stock Market Index besides A share stock
    indexes=["000001.SH","399001.SZ",'399006.SZ','000300.SH','000016.SH','000905.SH']
    #retrieveSingleFactor('close','2000-01-01','2017-05-31',stocks=indexes)
    # generateData("E:\\close_2002-2017.csv", datetime.date(2002,1,1),datetime.date(2017,5,31), indexes)

    # factors = ['open','high','low','close','volume','chg','pct_chg','vwap','dealnum']
    # option = "Period=W;Fill=Previous;PriceAdj=B"    # weekly data
    #高开低收、交易量、涨跌、涨跌幅、均价、成交笔数（成交额/成交量）
    # factors = ['open','high','low','volume','chg','pct_chg','vwap','dealnum'] # 注意 【收盘价】之前已经拉取了不再重复
    option = "Period=D;Fill=Previous;PriceAdj=B"    # daily data
    # retrieveMultipleFactors(factors, "2000-01-01", "2017-06-10", 'factors_day', indexes, option)
    # updateTransactionTime("2000-01-01", "2017-06-10",TYPE='W')
    calcMeanPrice("2000-01-01", "2017-06-10", indexes)
