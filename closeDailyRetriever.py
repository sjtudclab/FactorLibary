# pylint: disable=I0011,C0103,C0326,C0301, W0401,W0614

from datetime import datetime
from WindPy import *
import time

def retrieveSingleFactor(factor, startTime, endTime=datetime.today(), table='factors_day', option = "Period=D;Fill=Previous;PriceAdj=B"):
    # 启动Wind API
    w.start()

    # 获取所有的A股 IPO 目前共[3167]支
    wset = w.wset("SectorConstituent", u"sector=全部A股;field=wind_code")
    if wset.ErrorCode != 0:
        print("!!!===== WIND ERROR CODE: ", wset.ErrorCode)
        exit()
    stocks = wset.Data[0]
    print (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "Total A stocks number: ", len(stocks))

    # 获取交日 序列
    times = w.tdays(startTime, endTime, option).Times
    timeList = []
    for i in range(len(times)):
        row = str(times[i])
        row = row[:row.find(' ')]
        timeList.append(row)
    print(timeList)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) , 'Pulling Started...\n --------------------------')

    # 获取日数据收盘价（向后复权）
    # wind一次只能拉一只股票,数据写入Cassandra
    from cassandra.cluster import Cluster
    cluster = Cluster(['192.168.1.111'])
    session = cluster.connect('factors') # factors: factors_month
    sql = "INSERT INTO "+table
    preparedStmt = session.prepare(sql + "(stock, factor, time, value) VALUES (?,?,?,?)")

    for stock in stocks:
        wsd_data = w.wsd(stock, factor, startTime, endTime, option).Data
        for j in range(len(timeList)):
            #print (stock,factor,timeList[j],str(wsd_data[0][j]))
            session.execute_async(preparedStmt, (stock, factor,timeList[j],str(wsd_data[0][j])))

    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '---------------- Persistion finished!\n')

    #result testing
    print("---------- Inserstion Testing: ")
    rows = session.execute("select * from "+table+" where stock='600000.SH' and factor = '"+factor+"' and time > '2017-03-02'")
    for row in rows:
        print(row.stock,row.factor,row.time,row.value)
    # close connection with cassandra
    cluster.shutdown()

retrieveSingleFactor('close','2009-01-01')