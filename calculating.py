# pylint: disable=I0011,C0103,C0326,C0301, W0401,W0614
from cassandra.cluster import Cluster
from cassandra.util import Date
from datetime import timedelta
import time
import datetime
import math
## calculating ROA growth
# 1. get all the time series
# 2. mark the last date of the endYear
# 3. calulate ROA growth
# 4. insert into DB
## ROA: 今年此月的ROA / 去年的此月的ROA

def calculate_ROA(beginDate, endDate, factor_table = "factors_month"):
    #cassandra connection
    cluster = Cluster(['192.168.1.111'])
    session = cluster.connect('factors') #connect to the keyspace 'factors'

    # get stocks list with IPO_date
    # rows = session.execute('''SELECT stock, ipo_date FROM stock_info WHERE stock = '600444.SH' and trade_status = '1' ALLOW FILTERING ''')
    rows = session.execute('''SELECT stock, ipo_date FROM stock_info WHERE trade_status = '1' ALLOW FILTERING ''')
    stocks = {}
    for row in rows:
        stocks[row[0]] = row[1]

    # 得到去年的12个月的ROA值，以计算今年的ROA值，用375而不是365是为了留出空余给非交易日
    lastYearOfBeginDate = beginDate - timedelta(days=375)
    selectPreparedStmt = session.prepare("select time, value from "+factor_table+" where stock = ? and factor = 'roa' and time >= ? and time <= ? ALLOW FILTERING")
    preparedStmt = session.prepare("INSERT INTO "+factor_table+" (stock, factor, time, value) VALUES (?,'roa_growth', ?, ?)")
    #select all ROA value for each stock
    for stock, ipo_date in stocks.items():
        isOverlappd = False
        if lastYearOfBeginDate > ipo_date.date():
            begin = lastYearOfBeginDate  
        else:
            begin = ipo_date.date()
            isOverlappd = True
        rows = session.execute(selectPreparedStmt, (stock, str(begin), str(endDate)))
        growth = 0               # ROA_curr / ROA_prev[-12month]
        roa_growth = {}

        ## store historic ROA in tuple array
        roaMap = []
        for row in rows:
            roaMap.append((row.time, row.value))
            # print ((row.time.date().strftime("%Y-%m-%d"), row.value))

        ## calculate growth
        index = 0
        length = len(roaMap)
        # 1. IPO没越界, DB中前12个月无数据
        # 2. IPO越界，但小于BeginDate
        # 以第一个不小于BeginDate的交易日作为起点
        if isOverlappd == False or (isOverlappd == True and ipo_date.date() < beginDate):
            while index < length and roaMap[index][0].date() < beginDate:
                index += 1
        if index < length:
            print ("%s Begin: %s , IPO: %s index: %d , len: %d" % (stock, str(roaMap[index][0].date()), str(ipo_date.date()),index, length))

        # calculate
        for i in range(index, length):
            # 前面没有数据
            if i < 12:
                roa_growth[roaMap[i][0].date()] = 0
            else:
                prevRoa = roaMap[i-12][1]
                roa_growth[roaMap[i][0].date()] = roaMap[i][1] / prevRoa if prevRoa != 0 and math.isnan(prevRoa) == False else 0

        # insert to DB
        for item in roa_growth.items():
            session.execute_async(preparedStmt, (stock, item[0], item[1]))
            # print(item[0].strftime("%Y-%m-%d"), item[1])

        print (stock + " roa_growth calculation finished")

## Yield = Close(month_start) / Close(mont_end)
def calculate_Yield(beginDate, endDate, calc_table = "factors_day", store_table = "factors_month", TYPE="D"):
    # cassandra connection
    cluster = Cluster(['192.168.1.111'])
    session = cluster.connect('factors') #connect to the keyspace 'factors'

    # month begin & end time list
    sql="select * from transaction_time where type= '"+TYPE+ "' and time >= '"+ str(beginDate) +"' and time <= '" + str(endDate)+"'"
    print (sql)
    rows = session.execute(sql)
    dateList = []
    prevMonth = beginDate.month
    currMonth = prevMonth
    prevDay = None
    currDay = None
    cnt = 0
    # 筛选出月初和月末的日期
    for row in rows:
        prevDay = currDay
        prevMonth = currMonth
        # update
        currDay = row.time.date()
        if cnt == 0:
            dateList.append(currDay)
        currMonth = currDay.month
        #print('currDay: %s currentMonth: %s' % (currDay, currMonth))
        # month change
        if currMonth != prevMonth:
            if prevDay is not None:
                dateList.append(prevDay)
            dateList.append(currDay)
        cnt += 1
    # omit 1st one when it's the end of month
    print(str(cnt)+" Size of dateList: ",len(dateList))

    if dateList[1].month != beginDate.month:
        dateList = dateList[1:]
    print(dateList)
    # make it even
    if len(dateList) % 2 != 0:
        dateList = dateList[:-1]

    print(dateList)

    insertPreparedStmt = session.prepare("INSERT INTO "+store_table+" (stock, factor, time, value) VALUES (?,'Yield', ?, ?)")

    # get stocks list with IPO_date
    rows = session.execute('''SELECT stock, ipo_date FROM stock_info WHERE trade_status = '1' ALLOW FILTERING ''')
    stocks = {}
    for row in rows:
        stocks[row[0]] = row[1]

    #select all daily close price value for each stock
    #for stock in ["000852.SZ","603788.SH","603990.SH","603991.SH","603993.SH"]:
    for stock, ipo_date in stocks.items():
        sql = "select time, value from " + calc_table + " where stock = '"+stock+"' and factor = 'close' and time in ("
        for day in dateList:
            if day > ipo_date.date():        # delete invalid date
                sql += "'"+str(day)+"',"
        sql = sql[:-1] + ");"               # omit the extra comma
        rows = session.execute(sql)
        ## calculating close Yield
        # divid the first day's close price by the end day in the month

        prev = 1.0     # previous Yield value
        yield_map = {}
        cnt = 0
        for row in rows:
            # end of month, store the quotient
            if cnt % 2 > 0:
                yield_map[row.time] = float(row.value) / float(prev)
            # in case divided by 0
            elif row.value == 0:
                prev = 1.0
            else:
                prev = row.value
            #print ("cnt " + str(cnt)+" K: ", row[0]," V: ", row[1])
            cnt = cnt + 1
        # insert to DB
        for item in yield_map.items():
            session.execute_async(insertPreparedStmt, (stock, item[0], item[1]))
            # print(item[0].date().strftime("%Y-%m-%d"), item[1])
        print (stock + " Close Yield calculation finished")
    cluster.shutdown()

# 计算动量模块单独抽取出来，默认为1个月的动量，因为之后可能要计算两个月，三个月的动量
# mmt = Close(this month) / Close(last month)
def calculate_mmt(beginDate, endDate, factor_table = "factors_month", gap = 1):
    cluster = Cluster(['192.168.1.111'])
    session = cluster.connect('factors')

    # tradable stocks' collection
    rows = session.execute('''SELECT stock, ipo_date FROM stock_info WHERE trade_status = '1' ALLOW FILTERING ''')
    stocks = {}
    for row in rows:
        stocks[row[0]] = row[1]

    preparedStmt = session.prepare("INSERT INTO "+factor_table+" (stock, factor, time, value) VALUES (?,'mmt', ?, ?)")
    selectPreparedStmt = session.prepare("select time, value from "+factor_table+" where stock = ? and factor = 'close' and time >= ? and time <= ? ALLOW FILTERING")
    #select all close value for each stock every month
    for stock, ipo_date in stocks.items():
        begin = beginDate if beginDate > ipo_date.date() else ipo_date.date()
        rows = session.execute(selectPreparedStmt, (stock, str(begin), str(endDate)))
        ## calculating mmt
        cnt = 0
        curr = 0
        prev = 0            # previous close value
        mmt = 0             # Close_curr / Close_prev
        mmt_dic = {}
        for row in rows:
            prev = curr
            curr = row.value
            if curr is None or math.isnan(curr):
                curr = 0
            # in case divided by 0
            mmt = 0 if prev == 0 or cnt == 0 else curr / prev
            mmt_dic[row.time] = mmt
            cnt += 1
        # insert to DB
        for item in mmt_dic.items():
            session.execute_async(preparedStmt, (stock, item[0], item[1]))
            # print(item[0].date().strftime("%Y-%m-%d"), item[1])

        print (stock + " mmt calculation finished")

################################
#### Invoke Function  ##########
calculate_ROA(datetime.date(2009,1,1), datetime.datetime.today().date(), "factors_month")
# calculate_Yield(datetime.date(2009,1,1), datetime.datetime.today().date())
# calculate_mmt(datetime.date(2017,1,26), datetime.date(2017,3,31))
# calculate_ROA(datetime.date(2009,1,1), datetime.date(2017,4,1), "factors_month")
# calculate_Yield(datetime.date(2009,1,1), datetime.date(2017,4,1))
# calculate_mmt(datetime.date(2009,1,1), datetime.date(2017,4,1))
