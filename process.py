# -*- coding: UTF-8 -*-
# pylint: disable=I0011,C0111, C0103,C0326,C0301, C0304, W0401,W0614
from cassandra.cluster import Cluster
from cassandra.util import Date
import time
import datetime
import math

# from correct import delError
from dataMonthRetriever import monthRetrieve
from calculating import *
from sorting import sort_factors

## Execute the whole process automatically
## 1. retrieve newly updated data from last checkpoint
# monthRetrieve(datetime.date(2009,1,1), datetime.datetime.today().date(), 
#     fields1=['trade_status','close', 'mkt_freeshares','mkt_cap_float','roa'], multi_mfd = False)

# ## 2. calculating secondary factors
calculate_ROA_growth(datetime.date(2017,3,31), datetime.datetime.today().date(), "factors_month")
# calculate_Yield(datetime.date(2009,1,1), datetime.datetime.today().date())
# calculate_mmt(datetime.date(2009,1,1), datetime.datetime.today().date())


# sort_factors("2009-01-01", factors=['mkt_freeshares','mmt','roa_growth','Yield'])
sort_factors("2009-01-01", factors=['roa_growth'])