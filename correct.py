from cassandra.cluster import Cluster
cluster = Cluster(['192.168.1.111'])
session = cluster.connect('factors') # factors: factors_month
updatePreparedStmt = session.prepare('''INSERT INTO factors_month(stock, factor, time, value) VALUES (?,'mmt','2017-03-31',?)''')
deletePreparedStmt = session.prepare('''DELETE FROM factors_month WHERE stock = ? AND factor = ? AND time = '2017-03-28' ''')
delete31PreparedStmt = session.prepare('''DELETE FROM factors_month WHERE stock = ? AND factor = 'Yield' AND time = '2017-03-31' ''')
deleteYieldStmt = session.prepare('''DELETE FROM factors_month WHERE stock = ? AND factor = 'Yield' ''')
# rows = session.execute('''SELECT * FROM factors_month WHERE factor = 'mmt' AND time = '2017-03-28' ALLOW FILTERING''')
# cnt = 1
# for row in rows:
#     session.execute(updatePreparedStmt,(row.stock,row.value))
#     if cnt % 1000 == 0:
#         print(" Updating %d at %s" % (cnt, row.stock))
#     cnt += 1

cnt = 1
# rows = session.execute('''SELECT * FROM factors_month WHERE time = '2017-03-28' ALLOW FILTERING''')
# for row in rows:
#     session.execute(deletePreparedStmt,(row.stock,row.factor))
#     if cnt % 1000 == 0:
#         print(" Delete %d at %s" % (cnt, row.stock))
#     cnt += 1
rows = session.execute('''select stock from stock_info''')
for row in rows:
    # session.execute(delete31PreparedStmt,(row.stock,))
    session.execute(deleteYieldStmt,(row.stock,))
    if cnt % 1000 == 0:
        print(" Deleting %d at %s" % (cnt, row.stock))
    cnt += 1
cluster.shutdown()