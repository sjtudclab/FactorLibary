# -*- coding: UTF-8 -*-
# pylint: disable=I0011,C0111, C0103,C0326,C0301, C0304, W0401,W0614
from cassandra.cluster import Cluster
from cassandra.util import Date
import time
import datetime
import math

# from correct import delError
# from dataMonthRetriever import monthRetrieve
from calculating import *
from sorting import sort_factors

## Execute the whole process automatically

# delError()
# # retrieve newly updated data
# monthRetrieve("2017-03-01","2017-04-01")
# calculating
calculate_ROA(datetime.date(2009,1,1), datetime.date(2017,4,1), "factors_month")
calculate_Yield(datetime.date(2009,1,1), datetime.date(2017,4,1))
calculate_mmt(datetime.date(2009,1,1), datetime.date(2017,4,1))
sort_factors("2009-01-01", factors=['mkt_freeshares','mmt','roa_growth','Yield'])