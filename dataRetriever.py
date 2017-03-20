from WindPy import *

# 启动Wind API
w.start()

# 获取所有的A股 IPO > 3个月的股票

# 获取周数据（有些字段参数不同，需分成若干次拉取）
fields1 = ['windcode','sec_name','ipo_date','close', 'mkt_cap_float', 'mfd_buyamt_d', 'mfd_sellamt_d', 'roa', 'pe', 'pb']
# 定义某些指标需要的参数，收盘价向后复权
option = ["ruleType=8;unit=1;traderType=1;Period=W;Fill=Previous;PriceAdj=B"]

# 获取所有A股所需数据
print('Pull market quotation data: Start')
security = '600000.SH'
startTime = '2015-12-30'
endTime = '2017-03-20'

wsd_data = w.wsd(security, fields1, startTime, endTime, option)

for x in wsd_data.Fields:
        print('\t' + str(x))

print('ErrorCode: ' + str(wsd_data.ErrorCode))

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
print('Pull market quotation data: End')
