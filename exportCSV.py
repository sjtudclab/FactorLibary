# pylint: disable=I0011,C0111, C0103,C0326,C0301, C0304, W0401,W0614
from cassandra.cluster import Cluster
from cassandra.util import Date
import time
import datetime
import csv
import os

################################################################################################
## Select all valid stock with required factors identified by stock code & date, saved in CSV ##
################################################################################################
def export(fileName, beginDate, endDate=datetime.datetime.today().date(), factors = [], table = "factors_month"):
    if len(factors) == 0 or beginDate > endDate or len(fileName) == 0:
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
    SQL = "SELECT * FROM "+table+" WHERE stock = ? AND factor IN ("

    # sorting factors since they're ordered in cassandra
    factors = sorted(factors)
    print("Sorted factors: ", factors)
    #time list
    rows = session.execute('''
        select * from transaction_time 
        where type='month' and time >= %s and time <= %s ALLOW FILTERING;''', [beginDate, endDate])
    dateList = []
    for row in rows:
        dateList.append(row.time)

    #total stock number
    rows = session.execute("select count(*) from "+table+" where factor='close' and time = %s ALLOW FILTERING;", [str(dateList[0])])
    for row in rows:
        validStockNum = row[0]
        break
    rows = session.execute('''select count(*) from stock_info''')
    for row in rows:
        totalStockNum = row[0]
        break
    print ("Total Stock: ", totalStockNum, " Valid Stock: ", validStockNum)
    # prepare SQL
    for factor in factors:
        SQL = SQL + "'"+ factor + "',"
    SQL = SQL[:-1]
    SQL = SQL +") AND time = ?;"
    print (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), " PREPARE QUERY SQL: \n"+SQL)
    preparedStmt = session.prepare(SQL)

    # open CSV file & write first line: title 
    # NOTICE:  [wb] mode won't result in problem of blank line
    with open(fileName, 'w') as csvFile:
        names = ['id'] + ['Yield_Rank_Class'] + factors  # column names
        f = csv.writer(csvFile, delimiter=',',lineterminator='\n')
        f.writerow(names)

        # retrieve data
        for day in dateList:
            for stock in stocks:
                line = []
                rows = session.execute(preparedStmt, (stock,day))

                # pass when no data
                empty = True
                line.append(stock+'_' + str(day))
                for row in rows:
                    empty = False
                    if row.factor.find('rank') != -1:
                        rank = int(row.value / validStockNum * 1000) # normalize rank value

                        if row.factor.find('Yield') != -1:
                            rank = int(row.value / totalStockNum * 1000)
                            line.append(rank)
                            ##################################################
                            ####### CODE Area for Yield Rank Classification ##
                            ##################################################
                            # class 1: [1, 26]
                            if rank > 1 * 10 and rank < 26 * 10:
                                line.append(1)
                            # class 0: [74, 99]
                            elif rank > 74 * 10 and rank < 99 * 10:
                                line.append(0)
                            else:
                                line.append('') #no class, fill in empty char to keep CSV well-formed
                        else:
                            line.append(rank)
                    else:
                        line.append(str(row.value))
                if empty:
                    continue
                # write row
                f.writerow(line)

    # close connection with cassandra
    cluster.shutdown()
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), 'Writing to ',fileName,' complete!')

##############################################
################# USAGE EXAMPLE ##############
export('E:\\train.csv', datetime.date(2017,1,1),factors=['mkt_freeshares_rank', 'mmt_rank', 'roa_growth_rank','Yield_rank'])