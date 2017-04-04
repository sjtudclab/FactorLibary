'''
##Sorting Examples:
students=[('john', 'A', 15), ('jane', 'B', 12), ('dave', 'B', 10)]
#default ascending order
y = sorted(students, key=lambda student: student[2])
print (y,"\nAscending order by age: ",students)

import collections
Student = collections.namedtuple('Student', 'name grade age')
print ("Type of s is : ", type(Student))
s = Student(name = 'john', grade = 'A', age = 15)
print ("%s is %s years old, got an %s in Math" % (s.name, s.age, s.grade))
students=[Student(name = 'john', grade = 'C', age = 15), Student(name = 'Enna', grade = 'B', age = 12), Student(name = 'dave', grade = 'A', age = 10)]
print ("Descending order by age: ", sorted(students, key=lambda x: x.age, reverse = True))
print ("Ascending order by grade: ", sorted(students, key=lambda x: x.grade))

from operator import itemgetter, attrgetter
print ("Descending order by grade: ", sorted(students, key = itemgetter(1)))
print ("Ascending order by name: ", sorted(students, key = attrgetter('name')))
'''
# pylint: disable=I0011,C0103,C0326,C0301, W0401,W0614
from cassandra.cluster import Cluster
from cassandra.util import Date
from datetime import datetime

#sorting factors we need
# 1. get all the transaction date
# 2. for each time point, select all stock's factor value
# 3. sorting
# 4. index them & insert into DB
def sort_factors(beginDate, endDate=datetime.today().date(), factors = [], table = "factors_month", descending=True):
    if len(factors) == 0:
        return
    #cassandra connection
    cluster = Cluster(['192.168.1.111'])
    session = cluster.connect('factors') #connect to the keyspace 'factors'
    # get transaction date monthly
    rows = session.execute('''
        select * from transaction_time 
        where type='month' and time > %s and time < %s ALLOW FILTERING;''', [beginDate, endDate])
    dateList = []
    for row in rows:
        dateList.append(row.time)
    print(dateList)
    # prepare
    selectPreparedStmt = session.prepare(
        "select * from " + table + " where factor=? and time=? ALLOW FILTERING")
    insertPreparedStmt = session.prepare(
        "INSERT INTO " + table + "(stock, factor, time, value) VALUES (?,?,?,?)")

    # sort each factor for all stocks at each time step
    for factor in factors:
        for day in dateList:
            rows = session.execute(selectPreparedStmt, (factor, day))
            # 无数据跳过
            empty = True
            for x in rows:
                empty = False
                break
            if empty:
                continue
            #print (rows.currentRows)
            sortedRows = sorted(rows, key=lambda x: x.value, reverse=descending)
            cnt = 0
            for row in sortedRows:
                cnt += 1
                session.execute_async(insertPreparedStmt, (row.stock, factor + '_rank', row.time, cnt))
                #print(row.stock, factor + '_rank', row.time, cnt)
            print("%s - [ %s ] - complete sorting [ %d stocks]" % (day.date().strftime("%Y-%m-%d"), factor, cnt))
    # close connection with cassandra
    cluster.shutdown()

##############################################
################ Invoke Function #############
#sort_factors("2009-01-01", factors=['mkt_freeshares','mmt','roa_growth','mfd_buyamt_d1', 'mfd_sellamt_d1', 'roa', 'pe', 'pb','mfd_buyamt_d2', 'mfd_sellamt_d2','mfd_buyamt_d4', 'mfd_sellamt_d4'])
#sort_factors("2009-01-01", factors=['mkt_freeshares','mmt','roa_growth'])
sort_factors("2009-01-01", factors=['Yield'])

