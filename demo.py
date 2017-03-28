# cassandra connection
from cassandra.cluster import Cluster

#接口IP，需要做一个映射
cluster = Cluster(['202.120.40.111'])
session = cluster.connect('factors') #connect to the keyspace 'factors'
preparedStmt = session.prepare('''INSERT INTO factors_week(stock, factor, time, value) VALUES (?,?,?,?)''')
session.execute(preparedStmt, ('000852.SZ','toy','2017-03-02','2.57'))

#从周因子表中获取股票600000.SH在2017-03-02至今的所有因子
rows = session.execute("select * from factors_week where stock='600000.SH'and factor in ('close', 'roa') and time > '2017-03-02' ALLOW FILTERING;")

for row in rows:
    print(row.stock,row.factor,row.time,row.value)


