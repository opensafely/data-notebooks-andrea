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

# # Raw data checks for Waiting List Data - OpenPathways
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
# This notebook focuses on the WL_OpenPathways table, which is expected to represent patients on a waiting list at a given point in time.
#
# NHS England receives a weekly “Waiting List Minimum Dataset” (WL MDS) that is loaded on to the NCDR.  The waiting list data includes patients/pathways currently subject to Referral to Treatment (RTT) monitoring, as well as those not included. 

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
# limit to WL_OpenPathways
tables = list(tables_to_describe["tables"].keys())
table = tables[2]

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

# ### Number of patients by week

dat = simple_sql_dt(dbconn, table, col="Week_Ending_Date", cnt="count(distinct Patient_ID)", where=schema_filter, start='2021-05-01', end='2022-06-01')
pct(dat.row_count)

# ### Number of referral identifiers by week

dat = simple_sql_dt(dbconn, table, col="Week_Ending_Date", cnt="count(distinct Pseudo_Referral_Identifier)", where=schema_filter, start='2021-05-01', end='2022-06-01')
pct(dat.row_count)

# ### Number of pathway IDs by week

dat = simple_sql_dt(dbconn, table, col="Week_Ending_Date", cnt="count(distinct PSEUDO_PATIENT_PATHWAY_IDENTIFIER)", where=schema_filter, start='2021-05-01', end='2022-06-01')
pct(dat.row_count)

# ## Extract latest record for each individual

last = max_date(dbconn, table=table, where=schema_filter, start='2021-05-01', end='2022-06-01')

# #### Most common maximum dates

last.sort_values("row_count",ascending=False,inplace=True)
last = suppress_and_round2(last,field="row_count",keep=False)
last.head(10)

# ## Referral request received date
# ### Number of pathways by referral year
# Using pathways that ended on 2022-05-01.
# Restricted to referral years >=1999.

by_year(dbconn, table, col="REFERRAL_REQUEST_RECEIVED_DATE", cnt="count(distinct PSEUDO_PATIENT_PATHWAY_IDENTIFIER)", where=schema_filter)

# ## Referral_to_Treatment start date
# ### Number of pathways by RTT year
# Using pathways that ended on 2022-05-01.

by_year(dbconn, table, col="REFERRAL_TO_TREATMENT_PERIOD_START_DATE", cnt="count(distinct PSEUDO_PATIENT_PATHWAY_IDENTIFIER)", where=schema_filter)

# ## Current Pathway Period Start date

# ### Number of pathways by Current Pathway year
# Using pathways that ended on 2022-05-01

by_year(dbconn, table, col="Current_Pathway_Period_Start_Date", cnt="count(distinct PSEUDO_PATIENT_PATHWAY_IDENTIFIER)", where=schema_filter)

# ## Frequency distributions

# ### Waiting_List_Type

freq_dist(dbconn,table,col='Waiting_List_Type',where=schema_filter)

# ### Priority_Type_Code

freq_dist(dbconn,table,col='PRIORITY_TYPE_CODE',where=schema_filter)

# ### Inclusion on Cancer PTL

freq_dist(dbconn,table,col='Inclusion_on_Cancer_PTL',where=schema_filter)

# ### Outcome of Attendance Code

freq_dist(dbconn,table,col='OUTCOME_OF_ATTENDANCE_CODE',where=schema_filter)

# ### Proposed Procedure Opcs code

freq = freq_dist(dbconn,table,col='Proposed_Procedure_Opcs_Code',where=schema_filter)
freq.head(10)

# ### Main Specialty Code

freq = freq_dist(dbconn,table,col='MAIN_SPECIALTY_CODE',where=schema_filter)
freq.head(10)

# ### Activity Treatment Activity Code

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

# ## Date comparisons

# ### Compare referral date and Week_Ending_Date

compare_two_values(dbconn, table=table, 
                   columns=["Week_Ending_Date", "REFERRAL_REQUEST_RECEIVED_DATE"], 
                   threshold=1,
                   where="Week_Ending_Date IS NOT NULL AND REFERRAL_REQUEST_RECEIVED_DATE IS NOT NULL",
                   include_counts=True)

# ### Compare RTT date and Week_Ending_Date

compare_two_values(dbconn, table, 
                   columns=["Week_Ending_Date","REFERRAL_TO_TREATMENT_PERIOD_START_DATE"], 
                   threshold=1,
                   where="Week_Ending_Date IS NOT NULL AND REFERRAL_TO_TREATMENT_PERIOD_START_DATE IS NOT NULL",
                   include_counts=True)

# ### Compare Current Pathway Period start date and Week_Ending_Date

compare_two_values(dbconn, table=table, 
                   columns=["Week_Ending_Date", "Current_Pathway_Period_Start_Date"], 
                   threshold=1,
                   where="Week_Ending_Date IS NOT NULL AND Current_Pathway_Period_Start_Date IS NOT NULL",
                   include_counts=True)

# ### Compare Due Date and Week_Ending Date

compare_two_values(dbconn, table, 
                   columns=["Week_Ending_Date","Due_Date"], 
                   threshold=1,
                   where="Week_Ending_Date IS NOT NULL",
                   include_counts=True)

# ### Compare Cancellation date and Week_Ending_Date

compare_two_values(dbconn, table, 
                   columns=["Week_Ending_Date","CANCELLATION_DATE"], 
                   threshold=1,
                   where="Week_Ending_Date IS NOT NULL",
                   include_counts=True)
