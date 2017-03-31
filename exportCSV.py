# pylint: disable=I0011,C0111, C0103,C0326,C0301, C0304, W0401,W0614
from cassandra.cluster import Cluster
from cassandra.util import Date
from datetime import datetime
import time
import csv

################################################################################################
## Select all valid stock with required factors identified by stock code & date, saved in CSV ##
################################################################################################
def export(fileName, beginDate, endDate=datetime.today().date(), factors = [], table = "factors_month"):
    if len(factors) == 0:
        return
    # cassandra connection
    #cluster = Cluster(['192.168.1.111'])
    cluster = Cluster(['202.120.40.111'])
    session = cluster.connect('factors') #connect to the keyspace 'factors'

    # get valid stocks in A share
    rows = session.execute('''select stock from stock_info''')
    stocks = []
    for row in rows:
        stocks.append(row[0])
    SQL = "SELECT * FROM factors_month WHERE stock = ? AND factor IN ("

    # sorting factors since they're ordered in cassandra
    factors = sorted(factors)
    print("Sorted factors: ", factors)
    #time list
    rows = session.execute('''
        select * from transaction_time 
        where type='month' and time > %s and time < %s ALLOW FILTERING;''', [beginDate, endDate])
    dateList = []
    for row in rows:
        dateList.append(row.time)
    # prepare SQL
    for factor in factors:
        SQL = SQL + "'"+ factor + "',"
    SQL = SQL[:-1]
    SQL = SQL +") AND time = ?;"
    print (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), " PREPARE QUERY SQL: \n"+SQL)
    preparedStmt = session.prepare(SQL)

    # open CSV file & write first line: title
    with open(fileName, 'w') as csvFile:
        names = ['ID'] + factors
        f = csv.DictWriter(csvFile, fieldnames=names,restval=0)
        f.writeheader()
        
        colNum = len(factors)
        # retrieve data
        for day in dateList:
            dic = {}
            for stock in ['600000.SH']:
                rows = session.execute(preparedStmt, (stock,day))
                # pass when no data
                empty = True
                for x in rows:
                    empty = False
                    break
                if empty:
                    continue

                dic['ID'] = stock+'_' + str(day)
                for row in rows:
                    dic[row.factor] = str(row.value)
                # write row
                f.writerow(dic)

    # close connection with cassandra
    cluster.shutdown()
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), 'Writing to ',fileName,' complete!')

##############################################
################# USAGE EXAMPLE ##############
export('E:\\train.csv', '2017-01-01',factors=['mkt_freeshares_rank', 'mmt_rank', 'roa_growth_rank'])