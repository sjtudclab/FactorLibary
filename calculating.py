# pylint: disable=I0011,C0103,C0326,C0301, W0401,W0614
from cassandra.cluster import Cluster
from cassandra.util import Date
from datetime import datetime
import time
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
    #select all ROA value for each stock
    for stock in stocks:
        rows = session.execute('''select time, value from %s where stock = %s 
        and factor = 'roa' and time > %s and time < %s ALLOW FILTERING''', [factor_table, stock, beginDate, endDate])
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

################################
#### Invoke Function  ##########
calculate_ROA("2009-01-01", datetime.today().date(), "factors_month")
