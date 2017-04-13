# pylint: disable=I0011,C0103,C0326,C0301, W0401,W0614
from cassandra.cluster import Cluster
from WindPy import *
import time
import datetime

def monthRetrieve(startTime, endTime = datetime.datetime.today(),
fields1 = ['close', 'trade_status', 'mkt_freeshares','mkt_cap_float', 'mfd_buyamt_d', 'mfd_sellamt_d', 'roa', 'pe', 'pb'], 
option1 = "ruleType=8;unit=1;traderType=1;Period=M;Fill=Previous;PriceAdj=B", multi_mfd = True):
    # cassandra connect
    cluster = Cluster(['192.168.1.111'])
    session = cluster.connect('factors') # factors: factors_month

    # 启动Wind API
    w.start()

    # 获取可交易日
    times = w.tdays(startTime, endTime, "Period=M").Times
    timeList = []
    for i in range(len(times)):
        row = str(times[i])
        row = row[:row.find(' ')]
        timeList.append(row)
    print(timeList)

    # # 【解耦：迁移至stock.py，定期更新】判断数据有效性
    # 获取某个月份所有可交易的A股 （如此的话每次一支股票只拿一个数据，分多个时间点去拿，请求数目过多，改成批量拉取一支股票
    # 所有因子
    # stocks = w.wset("SectorConstituent", u"sector=全部A股;field=wind_code")
    # validStocks ={}
    # # Total stock: 3183 [2017-04-13]
    # print (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "Total A stocks number: ", len(stocks.Data[0]))

    # # stock status update statement
    # updateStmt = session.prepare('''INSERT INTO stock_info(stock, ipo_date, trade_status) VALUES (?,?,?)''')
    # 
    # #for stock in ["000852.SZ","603788.SH","603987.SH","603988.SH","603989.SH","603990.SH","603991.SH","603993.SH"]:
    # #for stock in ["000852.SZ","603788.SH","603990.SH","603991.SH","603993.SH"]:
    # for stock in stocks.Data[0]:
    #     ipo_status = w.wsd(stock, "ipo_date, trade_status", datetime.datetime.today())
    #     #print (ipo_status)
    #     try:
    #         days = (datetime.datetime.today() - ipo_status.Data[0][0]).days
    #         # trade_status 不能用一个变量表示，而是一个时序的因子，这里的0/1只能用区分IPO是否符合要求
    #         if  days > 90 and ipo_status.Data[1][0] == "交易":
    #         # if  days > 90:
    #             validStocks[stock] = ipo_status.Data[1][0]
    #             session.execute(updateStmt, (stock, ipo_status.Data[0][0], '1'))
    #         else:
    #             # set status 0
    #             session.execute(updateStmt, (stock, ipo_status.Data[0][0], '0'))
    #             print (" Set invalid data: ", stock, str(ipo_status.Data[0][0]))

    #     except TypeError:
    #         print (" -- Log TypeError at Stock: ", stock, " :\t", str(ipo_status.Data[0][0]))
    # Valid: 2819 [2017-04-13]
    # tradable stocks' collection
    rows = session.execute('''SELECT stock, ipo_date FROM stock_info WHERE trade_status = '1' ALLOW FILTERING ''')
    validStocks = {}
    validStockCode = []
    for row in rows:
        validStocks[row.stock] = row.ipo_date
        validStockCode.append(row.stock)

    validN = len(validStocks)
    print (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) , " valid stocks' number: ", validN)
    #print (validStocks)

    ## 拉取因子，分阶段拉取，拉完异步存DB
    if multi_mfd == True:
        columns = fields1 + ['mfd_buyamt_d2', 'mfd_sellamt_d2','mfd_buyamt_d4', 'mfd_sellamt_d4']
    else:
        columns = fields1
    dataList = [] #创建数组
    cnt = 0   #当前拉取了多少支股票
    index = 0 #上一次dump的位置，主要目的是通过此索引找到该股票代码
    CHUNK_SIZE = 300 #每一次异步dump的股票个数

    preparedStmt = session.prepare('''INSERT INTO factors_month(stock, factor, time, value) VALUES (?,?,?,?)''')
    print (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) , " ------ Starting to insert to DB")

    ## 遍历所有股票
    for stock,ipo_date in validStocks.items():
        # 只取 IPO 之后的数据
        start = startTime if startTime > ipo_date.date() else ipo_date.date() 

        # 同一个变量，参数不一样，需要分成几次拉取
        wsd_data = w.wsd(stock, fields1, start, endTime, option1).Data
        if multi_mfd == True:
            fields2 = ['mfd_buyamt_d', 'mfd_sellamt_d']
            option2 = "unit=1;traderType=2;Period=M;Fill=Previous;PriceAdj=B"
            wsd_data = wsd_data + w.wsd(stock, fields2, start, endTime, option2).Data
            option3 = "unit=1;traderType=4;Period=M;Fill=Previous;PriceAdj=B"
            wsd_data = wsd_data + w.wsd(stock, fields2, start, endTime, option3).Data

        ##【修改：计算动量模块单独移出来，为可扩展性】mmt = close_1 / close_2; 没有数据增长率为0
        # mmt = []
        # mmt.append(1)
        # for i in range(1, len(wsd_data[0])):
        #     if wsd_data[0][i] is not None and wsd_data[0][i] != 0:
        #         mmt.append(wsd_data[0][i] / wsd_data[0][i-1])
        #     else:
        #         mmt.append(float('nan'))
        # wsd_data.append(mmt)
        dataList.append(wsd_data)
        cnt += 1
        #阶段性异步导出 dump data asynchronously, 300 stocks / round
        if cnt % CHUNK_SIZE == 0:
            for s in range(index, cnt):
                for i in range(len(columns)):
                    for j in range(len(dataList[s - index][i])):
                        #print (validStocks[s],columns[i],timeList[j],dataList[s - index][i][j])
                        try:
                            value = float('nan')
                            if dataList[s - index][i][j] is not None:
                                value = float(dataList[s - index][i][j])
                        except (ValueError, TypeError, KeyError) as e:
                            value = float('nan')
                            print ("--Log ValueError in ", validStockCode[s],"\t",columns[i],"\t",str(timeList[j]),"\t",str(dataList[s - index][i][j]))
                            print (e)
                            print ("--------------------------------------------------------------------------")
                        except IndexError as e:
                            print ("--------------------------------------------------------------------------")
                            print("len s: %d, len i: %d, len j: %d ~ " %(cnt, len(columns),len(timeList)), (s-index,i,j))
                            print(e)
                        session.execute_async(preparedStmt, (validStockCode[s],columns[i],timeList[j], value))
            #记录上一次导出数据位置，清空buffer
            index = cnt
            dataList = []
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) ,'------ Dump NO.%d end at stock %s \n' % (cnt, stock))

    print ("---- Last chunk size: ", len(dataList))
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) ,'---------------- Pulling finished!\n')

    # 最后的剩余数据插入cassandra
    for s in range(index, cnt):
        for i in range(len(columns)):
            for j in range(len(dataList[s - index][i])):
                #print (validStocks[s],columns[i],timeList[j],dataList[s - index][i][j])
                try:
                    value = float('nan')
                    if dataList[s - index][i][j] is not None:
                        value = float(dataList[s - index][i][j])
                except (ValueError, TypeError, KeyError) as e:
                    value = float('nan')
                    print ("--Log ValueError in ", validStockCode[s],"\t",columns[i],"\t",str(timeList[j]),"\t",str(dataList[s - index][i][j]))
                    print (e)
                    print ("--------------------------------------------------------------------------")
                except IndexError as e:
                    print ("--------------------------------------------------------------------------")
                    print("len s: %d, len i: %d, len j: %d ~ " %(cnt, len(columns),len(timeList)), (s-index,i,j))
                    print(e)
                session.execute_async(preparedStmt, (validStockCode[s],columns[i],timeList[j], value))

    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '---------------- Persistion finished!\n')

    #result testing
    print("---------- Inserstion Testing: ")
    rows = session.execute("select * from factors_month where stock='000852.SZ' and factor = 'mmt' and time > '2017-03-02'")
    for row in rows:
        print(row.stock,row.factor,row.time,row.value)

    # close connection with cassandra
    cluster.shutdown()

# retrieve newly updated data
monthRetrieve(datetime.date(2009,1,1), datetime.datetime.today().date(), fields1=['mkt_cap_float','roa'], multi_mfd = False)