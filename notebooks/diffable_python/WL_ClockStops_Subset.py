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
# This notebook focuses on the WL_ClcokStops table. A subset of the population has been generated for exploration purposes, such as determining whether key columns are consistent over the pathway.
#
# NHS England receives a weekly “Waiting List Minimum Dataset” (WL MDS) that is loaded on to the NCDR.  The waiting list data includes patients/pathways currently subject to Referral to Treatment (RTT) monitoring, as well as those not included. 
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
# columns of interest
columns = ["Patient_ID",
    "Week_Ending_Date",
    "Waiting_List_Type",
    "REFERRAL_REQUEST_RECEIVED_DATE",
    "REFERRAL_TO_TREATMENT_PERIOD_START_DATE",
    "Cancellation_Date",
    "Opcs_Code",
    "Due_Date",
    "Pseudo_Referral_Identifier",
    "PSEUDO_PATIENT_PATHWAY_IDENTIFIER",
    "PSEUDO_ORGANISATION_CODE_PATIENT_PATHWAY_IDENTIFIER_ISSUER",
    "Priority_Type_Code"
]
# -


display(
md(f'''**This notebook was run on {date.today().strftime('%Y-%m-%d')}
    and reflects the dataset at this date.**
    ''')
)

# ## Create sample of data based on Patient_ID
#
# Select all patients where last two ID digits are 98. In theory this should represent a 1% sample. 

samp = sample(dbconn,columns=columns,table=table,where="where right(Patient_ID,2) = 98")
samp['Week_Ending_Date'] = pd.to_datetime(samp['Week_Ending_Date'], format='%Y-%m-%d',errors="coerce")     
samp['REFERRAL_REQUEST_RECEIVED_DATE'] = pd.to_datetime(samp['REFERRAL_REQUEST_RECEIVED_DATE'], format='%Y-%m-%d',errors="coerce")     
samp = samp[(samp['Week_Ending_Date']<'2022-06-01') & (samp['REFERRAL_REQUEST_RECEIVED_DATE']>'2021-05-01')]

# #### Number of rows in sample

len(samp.index)

# ### Number of unique values in sample

samp.nunique()

# ### Number of unique Patient_ID/Pathway combinations

len(samp.groupby(['Patient_ID','PSEUDO_PATIENT_PATHWAY_IDENTIFIER']).size().index)

# ### Number of rows per unique Patient_ID/Pathway combination

samp2=samp.groupby(['Patient_ID',"PSEUDO_PATIENT_PATHWAY_IDENTIFIER"])['row_count'].sum().reset_index(name="week_cnt")

# #### Percentiles of no. rows

pct(samp2.week_cnt)

# ### Number of pathways per Patient_ID

samp2=samp.groupby(['Patient_ID'])["PSEUDO_PATIENT_PATHWAY_IDENTIFIER"].nunique().reset_index(name="path_cnt")

# #### Percentiles for no. pathways

pct(samp2.path_cnt)

# ### Check if waiting_list_type changes over patient/pathway
#
# #### Percentiles for number of unique waiting list type by pathway

samp2=samp.groupby(['Patient_ID',"PSEUDO_PATIENT_PATHWAY_IDENTIFIER"])["Waiting_List_Type"].nunique().reset_index(name="typ_cnt")
pct(samp2.typ_cnt)

# ### Check if priority_type_code changes over patient/pathway
#
# #### Percentiles for number of unique waiting list type by pathway

samp2=samp.groupby(['Patient_ID',"PSEUDO_PATIENT_PATHWAY_IDENTIFIER"])["Priority_Type_Code"].nunique().reset_index(name="prior_cnt")
pct(samp2.prior_cnt)

# ### Check if Referral Request date varies by patient/pathway
#
# #### Percentiles for number of unique referral dates by pathway

samp2=samp.groupby(['Patient_ID',"PSEUDO_PATIENT_PATHWAY_IDENTIFIER"])["REFERRAL_REQUEST_RECEIVED_DATE"].nunique().reset_index(name="rrrd_cnt")
pct(samp2.rrrd_cnt)

# ### Check if RTT date varies by pathway
#
# #### Percentiles for number of unique referral dates by pathway

samp2=samp.groupby(['Patient_ID',"PSEUDO_PATIENT_PATHWAY_IDENTIFIER"])["REFERRAL_TO_TREATMENT_PERIOD_START_DATE"].nunique().reset_index(name="rrt_cnt")
pct(samp2.rrt_cnt)

# ### Calculate difference between referral date and last date (Week_Ending_Date)
#
# i.e. how long people have been on waiting list. Restrict to people with non-null values and non-negative values.

# +
samp['Diff'] = 0
samp.loc[(pd.notnull(samp['Week_Ending_Date'])) & pd.notnull(samp['REFERRAL_REQUEST_RECEIVED_DATE']),'Diff'] = (samp['Week_Ending_Date'] - samp['REFERRAL_REQUEST_RECEIVED_DATE']).dt.days
samp['Diff'] = samp['Diff'].astype(int)
samp = samp[(samp['Diff']>=0)]

pct(samp.Diff)
# -

# #### Histogram of time between referral and end date
#
# Note: display truncated at 52 weeks

hist=samp.groupby(["Diff"])["Diff"].count().reset_index(name="n")
hist=suppress_and_round2(hist, field="n", keep=False)
hist=hist[(hist["Diff"]<53)]

# ##### Frequency distribution for chart

display(hist)

# Plot of records over time
ax=plt.subplot(111)
ax.bar(hist.Diff,hist.n,width=1)
plt.show()
