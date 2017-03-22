# pylint: disable=I0011,C0103,C0326,C0301, W0401,W0614

from datetime import datetime
from WindPy import *

# 启动Wind API
w.start()

# 获取所有的A股 IPO > 3个月的股票 目前共[3150]支，有[3014]支符合要求
stocks = w.wset("SectorConstituent", u"sector=全部A股;field=wind_code")
validStocks =[]
print ("Total A stocks number: ", len(stocks.Data[0]))
#for stock in stocks.Data[0]:
#for stock in ["000852.SZ","603788.SH","603987.SH","603988.SH","603989.SH","603990.SH","603991.SH","603993.SH"]:
for stock in ["603990.SH","603991.SH","603993.SH"]:
    ipo_status = w.wsd(stock, "ipo_date, trade_status", datetime.today())
    #print (ipo_status)
    if (datetime.today() - ipo_status.Data[0][0]).days > 90 and ipo_status.Data[1][0] == "交易":
        validStocks.append(stock)

validN = len(validStocks)
print ("valid stocks' number: ", validN)

# 获取周数据（有些字段参数不同，需分成若干次拉取）
#fields1 = ['windcode','sec_name','ipo_date','close', 'mkt_cap_float', 'mfd_buyamt_d', 'mfd_sellamt_d', 'roa', 'pe', 'pb']
fields1 = ['close','mkt_cap_float','mfd_buyamt_d', 'mfd_sellamt_d', 'roa', 'pe', 'pb']
# 定义某些指标需要的参数，收盘价向后复权， 流入流出为机构
option1 = "ruleType=8;unit=1;traderType=1;Period=W;Fill=Previous;PriceAdj=B"

print('Pulling Started...\n --------------------------')

startTime = '2017-3-15'
endTime = datetime.today() # 可缺省，默认今天

times = w.tdays(startTime, endTime, "Period=W").Times
timeList = []
for i in range(len(times)):
    row = str(times[i])
    row = row[:row.find(' ')]
    timeList.append(row)
print(timeList)

dataList = [] #创建数组
for stock in validStocks:
    wsd_data = w.wsd(stock, fields1, startTime, endTime, option1).Data
    #print (wsd_data)
    fields2 = ['mfd_buyamt_d', 'mfd_sellamt_d']
    option2 = "unit=1;traderType=2;Period=W;Fill=Previous;PriceAdj=B"
    wsd_data = wsd_data + w.wsd(stock, fields2, startTime, endTime, option2).Data
    #print (wsd_data)
    option3 = "unit=1;traderType=4;Period=W;Fill=Previous;PriceAdj=B"
    wsd_data = wsd_data + w.wsd(stock, fields2, startTime, endTime, option3).Data
    #print (wsd_data)
    dataList.append(wsd_data)

print (dataList)

# 插入cassandra
columns = ['close','mkt_cap_float','mfd_buyamt_d1', 'mfd_sellamt_d1', 'roa', 'pe', 'pb','mfd_buyamt_d2', 'mfd_sellamt_d2','mfd_buyamt_d4', 'mfd_sellamt_d4']
for s in range(len(validStocks)):
    for i in range(len(columns)):
        for j in range(len(timeList)):
            sql = "insert into stock_factors_week ('stock','factor','time','value') values (" + validStocks[s] + ',' + columns[i] + ',' + timeList[j] +','+str(dataList[s][i][j])+')'
            print (sql)

# #获取数据，行列反过来的
# rows = len(wsd_data.Data[0])
# cols = len(wsd_data.Data)
# print ("rows: %d ; columns: %d" % (rows, cols))

# col_name = '\t'
# for x in wsd_data.Fields:
#     col_name = col_name + str(x) + '\t'
# print('\n'+col_name)

# #print (wsd_data.Data)

# for y in range(rows):
#     row = str(wsd_data.Times[y])
#     row = row[:row.find(' ')]
#     for x in range(cols):
#         if x == 2:
#              time = str(wsd_data.Data[x][y])
#              time = time[:time.find(' ')]
#              row = row + '\t' + time
#         elif wsd_data.Data[x][y] is not None:
#             row = row + '\t' + str(wsd_data.Data[x][y])
#     print(row)
#     row = ""
# print('\n--------------- Done ----------------')

# if wsd_data.ErrorCode != 0:
#     print('ErrorCode: ' + str(wsd_data.ErrorCode))

# 生成存入Cassandra的代码，异步执行数据存储，排名计算及定期更新


'''
filename = 'wsd_' + security + '_' + year + '.csv'
with open(filename, 'w', encoding='utf-8', newline='') as f:
    f.write('DateTime')
    for x in wsd_data.Fields:
        f.write('\t' + str(x))
    f.write('\n')
    columns = len(wsd_data.Data)
    index = len(wsd_data.Data[0])
    for x in range(index):
        time = str(wsd_data.Times[x])
        f.write(time[:time.find(' ')])
        for y in range(columns):
            f.write('\t')
            if wsd_data.Data[y][x] is not None:
                f.write(str(wsd_data.Data[y][x]))
        f.write('\n')

'''
print('\n----------------\n Pulling finished!')
