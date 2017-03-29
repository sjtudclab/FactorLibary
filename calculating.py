# pylint: disable=I0011,C0103,C0326,C0301, W0401,W0614
from cassandra.cluster import Cluster
from cassandra.util import Date
import time
## calculating ROA growth
# 1. get all the time series
# 2. mark the last date of the endYear
# 3. calulate ROA growth
# 4. insert into DB
##

def calculate_ROA(beginDate, endDate):
    #cassandra connection
    cluster = Cluster(['192.168.1.111'])
    session = cluster.connect('factors') #connect to the keyspace 'factors'

    rows = session.execute('''
    select * from transaction_time 
    where type='month' and time > %s and time < %s ALLOW FILTERING;''',[beginDate, endDate])
    dateMap = {}
    for row in rows:
        date = row.time.date()
        if date.month == 12:
            dateMap[date.year] = date
    print(dateMap)

    # select all ROA value at the end of year
    # calculate ROA Growth, then insert
calculate_ROA("2009-01-01","2017-01-01")
