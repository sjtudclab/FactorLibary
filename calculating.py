# pylint: disable=I0011,C0103,C0326,C0301, W0401,W0614
from cassandra.cluster import Cluster
from cassandra.util import Date
import time
import datetime
import math
## calculating ROA growth
# 1. get all the time series
# 2. mark the last date of the endYear
# 3. calulate ROA growth
# 4. insert into DB
## ROA: 去年的年末ROA / 前年的年末ROA

def calculate_ROA(beginDate, endDate, factor_table = "factors_month"):
    #cassandra connection
    cluster = Cluster(['192.168.1.111'])
    session = cluster.connect('factors') #connect to the keyspace 'factors'

    # rows = session.execute('''
    # select * from transaction_time 
    # where type='month' and time > %s and time < %s ALLOW FILTERING;''',[beginDate, endDate])
    # dateMap = {}
    # for row in rows:
    #     date = row.time.date()
    #     if date.month == 12:
    #         dateMap[date.year] = date

    # sql = '''select * from factors_month where stock = ? and factor = 'roa' and time in ('''
    # for day in dateMap.values():
    #     sql = sql + "'"+ day.strftime("%Y-%m-%d")+ "'"+","
    # sql = sql[:-1]
    # sql = sql +");"
    # print (sql)

    # get stocks list with IPO_date
    rows = session.execute('''SELECT stock, ipo_date FROM stock_info WHERE trade_status = '1' ALLOW FILTERING ''')
    stocks = {}
    for row in rows:
        stocks[row[0]] = row[1]

    preparedStmt = session.prepare("INSERT INTO "+factor_table+" (stock, factor, time, value) VALUES (?,'roa_growth', ?, ?)")
    selectPreparedStmt = session.prepare("select time, value from "+factor_table+" where stock = ? and factor = 'roa' and time >= ? and time <= ? ALLOW FILTERING")
    #select all ROA value for each stock
    for stock, ipo_date in stocks.items():
        begin = beginDate if beginDate > ipo_date.date() else ipo_date.date()
        rows = session.execute(selectPreparedStmt, (stock, str(begin), str(endDate)))
        ## calculating ROA Growth
        cnt = 0
        prev = 1.0               # previous ROA value
        growth = 0               # ROA_curr / ROA_prev
        roa_growth = {}
        for row in rows:
            # divide when data change
            if cnt > 0 and row.value != prev:
                growth = row.value / prev
            # in case divided by 0
            if row.value == 0:
                prev = 1.0
            else:
                prev = row.value
            roa_growth[row.time] = growth
            cnt += 1
        # insert to DB
        for item in roa_growth.items():
            session.execute_async(preparedStmt, (stock, item[0], item[1]))
            #print(item[0].date().strftime("%Y-%m-%d"), item[1])

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
#calculate_ROA(datetime.date(2017,3,1), datetime.datetime.today().date(), "factors_month")
# calculate_Yield(datetime.date(2009,1,1), datetime.datetime.today().date())
# calculate_mmt(datetime.date(2017,1,26), datetime.date(2017,3,31))
# calculate_ROA(datetime.date(2009,1,1), datetime.date(2017,4,1), "factors_month")
# calculate_Yield(datetime.date(2009,1,1), datetime.date(2017,4,1))
calculate_mmt(datetime.date(2009,1,1), datetime.date(2017,4,1))
