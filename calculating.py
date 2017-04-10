# pylint: disable=I0011,C0103,C0326,C0301, W0401,W0614
from cassandra.cluster import Cluster
from cassandra.util import Date
from datetime import datetime
import time
import datetime
## calculating ROA growth
# 1. get all the time series
# 2. mark the last date of the endYear
# 3. calulate ROA growth
# 4. insert into DB
##

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

    # get stocks list
    rows = session.execute('''select stock from stock_info''')
    stocks = []
    for row in rows:
        stocks.append(row[0])

    preparedStmt = session.prepare("INSERT INTO "+factor_table+" (stock, factor, time, value) VALUES (?,'roa_growth', ?, ?)")
    selectPreparedStmt = session.prepare("select time, value from "+factor_table+" where stock = ? and factor = 'roa' and time >= ? and time <= ? ALLOW FILTERING")
    #select all ROA value for each stock
    for stock in stocks:
        rows = session.execute(selectPreparedStmt, (stock, str(beginDate), str(endDate)))
        ## calculating ROA Growth
        cnt = 0
        prev = 1.0               # previous ROA value
        growth = float('nan')    # ROA_curr / ROA_prev
        roa_growth = {}
        for row in rows:
            if cnt > 0 and row[1] != prev:
                growth = row[1] / prev
            # in case divided by 0
            if row[1] == 0:
                prev = 1.0
            else:
                prev = row[1]
            roa_growth[row[0]] = growth
            cnt += 1
        # insert to DB
        for item in roa_growth.items():
            session.execute_async(preparedStmt, (stock, item[0], item[1]))
            #print(item[0].date().strftime("%Y-%m-%d"), item[1])
        cluster.shutdown()
        print (stock + " roa_growth calculation finished")

## Yield = Close(month_start) / Close(mont_end)
def calculate_Yield(beginDate, endDate, calc_table = "factors_day", store_table = "factors_month", TYPE="D"):
    # cassandra connection
    cluster = Cluster(['192.168.1.111'])
    session = cluster.connect('factors') #connect to the keyspace 'factors'
    # get stocks list
    rows = session.execute('''select stock from stock_info''')
    stocks = []
    for row in rows:
        stocks.append(row[0])
    # month begin & end time list
    sql="select * from transaction_time where type= '"+TYPE+ "' and time >= '"+ datetime.datetime.strftime(beginDate,"%Y-%m-%d") +"' and time <= '" + datetime.datetime.strftime(endDate,"%Y-%m-%d")+"'"
    print (sql)
    rows = session.execute(sql)
    dateList = []
    prevMonth = beginDate.month
    currMonth = prevMonth
    prevDay = None
    currDay = None
    cnt = 0
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
    sql = "select time, value from " + calc_table + " where stock = ? and factor = 'close' and time in ("
    for day in dateList:
        sql += "'"+str(day)+"',"
    sql = sql[:-1] + ");"  # omit the extra comma
    print(sql)
    selectPreparedStmt = session.prepare(sql)
    insertPreparedStmt = session.prepare("INSERT INTO "+store_table+" (stock, factor, time, value) VALUES (?,'Yield', ?, ?)")

    #select all daily close price value for each stock
    #for stock in ["000852.SZ","603788.SH","603990.SH","603991.SH","603993.SH"]:
    for stock in stocks:
        rows = session.execute(selectPreparedStmt, (stock,))
        ## calculating close Yield
        # divid the first day's close price by the end day in the month

        prev = 1.0     # previous Yield value
        yield_map = {}
        cnt = 0
        for row in rows:
            # end of month
            if cnt % 2 > 0:
                yield_map[row[0]] = float(row[1]) / float(prev)
            # in case divided by 0
            if row[1] == 0:
                prev = 1.0
            else:
                prev = row[1]
            #print ("cnt " + str(cnt)+" K: ", row[0]," V: ", row[1])
            cnt = cnt + 1
        # insert to DB
        for item in yield_map.items():
            session.execute_async(insertPreparedStmt, (stock, item[0], item[1]))
            #print(item[0].date().strftime("%Y-%m-%d"), item[1])
        #cluster.shutdown()
        print (stock + " Close Yield calculation finished")

################################
#### Invoke Function  ##########
calculate_ROA(datetime.date(2017,3,1), datetime.datetime.today().date(), "factors_month")
calculate_Yield(datetime.date(2017,3,1), datetime.datetime.today().date())
