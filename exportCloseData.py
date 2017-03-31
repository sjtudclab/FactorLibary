# pylint: disable=I0011,C0111, C0103,C0326,C0301, C0304, W0401,W0614
from cassandra.cluster import Cluster
from cassandra.util import Date
from datetime import datetime
import time

# 启动Wind API
w.start()

# 获取每个时间点所有的A股的股票
stocks = w.wset("SectorConstituent", u"sector=全部A股;field=wind_code")
print (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "Total A stocks number: ", len(stocks.Data[0]))

# 获取日数据收盘价（向后复权）
field = 'close'
option = "Period=D;Fill=Previous;PriceAdj=B"

print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) , 'Pulling Started...\n --------------------------')

startTime = '2009-01-01'
endTime = datetime.today() # 可缺省，默认今天
times = w.tdays(startTime, endTime, "Period=D").Times
timeList = []
for i in range(len(times)):
    row = str(times[i])
    row = row[:row.find(' ')]
    timeList.append(row)
print(timeList)

# 拉取数据
dataList = [] 
# wind一次只能拉一只股票
for stock in stocks:
    wsd_data = w.wsd(stock, field, startTime, endTime, option).Data
    dataList.append(wsd_data)

colNum = len(stocks)
rowNum = len(timeList)
# 数据写入文件中
f = open("E:\\close.txt", "w", encoding='utf-8', newline='')
f.write(colNum)
f.write('\t')
f.write(rowNum)
f.write('\n')
f.write('close')
for stock in stocks:
	f.write(stock + '\t')
f.write('\n')

for i in range(rowNum):
	f.write(str(timeList[i]))
	for s in range(colNum):
        #print (timeList[i],stocks[s],dataList[s][0][i])
        f.write(dataList[s][0][i] + '\t')
    f.write('\n')

f.close()
print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '---------------- Persistion finished!\n')
