# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: all
#     notebook_metadata_filter: all,-language_info
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.3
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Raw data checks for Waiting List Data - ClockStops
#
# ### Tables
# The waiting list data consists of 3 tables:
# * WL_ClockStops
# * WL_Diagnostics
# * WL_OpenPathways
#
# ### Background and documentation
# * [Github issue](https://github.com/opensafely-core/cohort-extractor/issues/783)
# * [Background](https://docs.google.com/document/d/1kVF7hPhy8vv2_tA2aRv3j36Na9fD8YlE6Mj3KbUFx4o/edit)
# * [Schema description](https://docs.google.com/spreadsheets/d/1A1h6WGKXzh8Wy4qPMz4W2K7BrAIsSSMC/edit#gid=438381057)
# * [Recording and reporting guidelines](https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2021/05/Recording-and-Reporting-guidance-April_2021.pdf)
#
# ### Methods
# The waiting list dataset has been linked to patients in OpenSAFELY-TPP, covering 40% of England's population.
#
# This notebook focuses on the WL_Clockstops table.
#
# NHS England receives a weekly “Waiting List Minimum Dataset” (WL MDS) that is loaded on to the NCDR.  The waiting list data includes patients/pathways currently subject to Referral to Treatment (RTT) monitoring, as well as those not included. 
#
#

# +
import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime as dt
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()
from IPython.display import HTML
from IPython.display import Markdown as md
from IPython.core.display import HTML as Center
from IPython.display import Image, display
# %matplotlib inline
import pyodbc
from datetime import date, datetime

sys.path.append('../analysis/')
# from utilities import *
from sense_checking import *
from config_wl import tables_to_describe, duplicates, schema_filter
from utilities import *
from as_code import *

pd.set_option('display.max_colwidth', 250)

# get the server credentials
dbconn = os.environ.get('FULL_DATABASE_URL', None).strip('"')
# limit to WL_ClockStops
tables = list(tables_to_describe["tables"].keys())
table = tables[0]
# -


display(
md(f'''**This notebook was run on {date.today().strftime('%Y-%m-%d')}
    and reflects the dataset at this date, 
    but has been filtered to `{schema_filter}`.**
    ''')
)

# ## Total row count (Unfiltered table)

total_counts(dbconn, table=table, where="")

# ## Exploration of Week_Ending_Date
# ### Total row counts by Week_Ending_Date
#
# #### Weekly count percentiles

dat = simple_sql_dt(dbconn, table, col="Week_Ending_Date", cnt="count(*)", where=schema_filter, start='2021-05-01', end='2022-06-01')
pct(dat.row_count)

# #### Counts by month

dat['Month']=dat['Week_Ending_Date'].values.astype('datetime64[M]')
dat_mon=dat.groupby(['Month'])['row_count'].sum().reset_index(name='row_count')
print(dat_mon)

# #### Histogram of counts by month

ax=plt.subplot(111)
ax.bar(dat_mon.Month,dat_mon.row_count,width=10)
ax.xaxis_date()
plt.gcf().autofmt_xdate()
plt.show()

# ### Number of unique Patient_IDs by week
#
# #### Weekly count percentiles

dat = simple_sql_dt(dbconn, table, col="Week_Ending_Date", cnt="count(distinct Patient_ID)", where=schema_filter, start='2021-05-01', end='2022-06-01')
pct(dat.row_count)

# ### Number of referral identifier by week

dat = simple_sql_dt(dbconn, table, col="Week_Ending_Date", cnt="count(distinct Pseudo_Referral_Identifier)", where=schema_filter, start='2021-05-01', end='2022-06-01')
pct(dat.row_count)

# ### Number of pathway IDs by week

dat = simple_sql_dt(dbconn, table, col="Week_Ending_Date", cnt="count(distinct PSEUDO_PATIENT_PATHWAY_IDENTIFIER)", where=schema_filter, start='2021-05-01', end='2022-06-01')
pct(dat.row_count)

# ## Referral_Request_Received_Date
# ### Total pathways by referral year

dat = simple_sql_dt(dbconn, table, col="REFERRAL_REQUEST_RECEIVED_DATE", cnt="count(distinct PSEUDO_PATIENT_PATHWAY_IDENTIFIER)", where=schema_filter, start='1998-12-31', end='2022-06-01')
dat['Year']=pd.DatetimeIndex(dat['REFERRAL_REQUEST_RECEIVED_DATE']).year
dat.groupby(['Year'])['row_count'].sum()

# ### Total referral dates by month

dat['Month']=dat['REFERRAL_REQUEST_RECEIVED_DATE'].values.astype('datetime64[M]')
dat_mon=dat.groupby(['Month'])['row_count'].sum().reset_index(name='row_count')
dat_mon=dat_mon[(dat_mon['Month']<'2022-06-01') & (dat_mon['Month']>'2018-12-31')]
print(dat_mon)

# #### Histogram of monthly counts

ax=plt.subplot(111)
ax.bar(dat_mon.Month,dat_mon.row_count,width=20)
ax.xaxis_date()
plt.gcf().autofmt_xdate()
plt.show()

# ## Compare Referral date and Week_Ending_Date
# Note: restrict referral dates to past 5 years

compare_two_values(dbconn, table=table, 
                   columns=["Week_Ending_Date", "REFERRAL_REQUEST_RECEIVED_DATE"], 
                   threshold=1,
                   where="Week_Ending_Date IS NOT NULL",
                   include_counts=True)

# ## Referral_to_Treatment start date
# ### Number of pathways by year

dat = simple_sql_dt(dbconn, table, col="REFERRAL_TO_TREATMENT_PERIOD_START_DATE", cnt="count(distinct PSEUDO_PATIENT_PATHWAY_IDENTIFIER)", where=schema_filter, start='1998-12-31', end='2022-06-01')
dat['Year']=pd.DatetimeIndex(dat['REFERRAL_TO_TREATMENT_PERIOD_START_DATE']).year
dat.groupby(['Year'])['row_count'].sum()

# ### Total pathways by RTT month

dat['Month']=dat['REFERRAL_TO_TREATMENT_PERIOD_START_DATE'].values.astype('datetime64[M]')
dat_mon=dat.groupby(['Month'])['row_count'].sum().reset_index(name='row_count')
dat_mon=dat_mon[(dat_mon['Month']<'2022-06-01') & (dat_mon['Month']>'2018-12-31')]
print(dat_mon)

ax=plt.subplot(111)
ax.bar(dat_mon.Month,dat_mon.row_count,width=20)
ax.xaxis_date()
plt.gcf().autofmt_xdate()
plt.show()

# ## Compare RTT date and Week_Ending_Date

compare_two_values(dbconn, table, 
                   columns=["Week_Ending_Date","REFERRAL_TO_TREATMENT_PERIOD_START_DATE"], 
                   threshold=1,
                   where="Week_Ending_Date IS NOT NULL",
                   include_counts=True)

# ## Compare cancellation date and Week_Ending_Date

compare_two_values(dbconn, table, 
                   columns=["Week_Ending_Date","CANCELLATION_DATE"], 
                   threshold=1,
                   where="Week_Ending_Date IS NOT NULL",
                   include_counts=True)

# ## Compare due date and Week_Ending_Date

compare_two_values(dbconn, table, 
                   columns=["Week_Ending_Date","Due_Date"], 
                   threshold=1,
                   where="Week_Ending_Date IS NOT NULL",
                   include_counts=True)

# ## Frequency distributions - 10 most common codes

# ### Waiting_List_Type

freq = freq_dist(dbconn,table,col='Waiting_List_Type',where=schema_filter)
freq.head(10)

# ### Priority_Type_Code

freq = freq_dist(dbconn,table,col='Priority_Type_Code',where=schema_filter)
freq.head(10)

# ### Inclusion on Cancer PTL

freq = freq_dist(dbconn,table,col='Inclusion_on_Cancer_PTL',where=schema_filter)
freq.head(10)

# ### Outcome of Attendance Code

freq = freq_dist(dbconn,table,col='OUTCOME_OF_ATTENDANCE_CODE',where=schema_filter)
freq.head(10)

# ### Source of Referral for Outpatients

freq = freq_dist(dbconn,table,col='SOURCE_OF_REFERRAL_FOR_OUTPATIENTS',where=schema_filter)
freq.head(10)

# ### Main Specialty Code

freq = freq_dist(dbconn,table,col='MAIN_SPECIALTY_CODE',where=schema_filter)
freq.head(10)

# ### Activity Treatment Function Code

freq = freq_dist(dbconn,table,col='ACTIVITY_TREATMENT_FUNCTION_CODE',where=schema_filter)
freq.head(10)

# ### Procedure Priority Code

freq = freq_dist(dbconn,table,col='Procedure_Priority_Code',where=schema_filter)
freq.head(10)

# ### Diagnostic Priority Code

freq = freq_dist(dbconn,table,col='Diagnostic_Priority_Code',where=schema_filter)
freq.head(10)

# ### Outpatient Priority Code

freq = freq_dist(dbconn,table,col='Outpatient_Priority_Code',where=schema_filter)
freq.head(10)

# ### Completed Type

freq = freq_dist(dbconn,table,col='Completed_Type',where=schema_filter)
freq.head(10)
