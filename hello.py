# import datetime
# begin = datetime.date(2014, 6, 1)
# end = datetime.datetime.now()
# end = datetime.date(end.year, end.month, end.day)
# d = begin
# delta = datetime.timedelta(days=1)
# while d <= end:
#     print(d.strftime("%Y-%m-%d"))
#     d += delta
# print(end)
# for i in range(1,5):
i = 1
while i < 5:
    print(i)
    if i == 2:
        i = 4
    i+=1
