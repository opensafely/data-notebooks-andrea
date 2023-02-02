import os
from datetime import date, datetime
import sys

from IPython.display import display, Markdown
import numpy as np
import pandas as pd
import pyodbc
from tqdm import tqdm

sys.path.append('../lib/')
from utilities import *


def total_counts(dbconn, table, where):
    where_str = f" filtered on {where}" if where else ""
    with pyodbc.connect(dbconn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"select Count(*) from {table} {where}")
            result = cur.fetchone()
            display(Markdown(f"Total rows: {result[0]}"))

def cnt_per_date(dbconn, table, col, col2, where):
    where_str = f" filtered on {where}" if where else ""
    with closing_connection(dbconn) as cnxn:
        ppl = pd.read_sql(f"select {col}, count(distinct {col2}) as row_count from WL_ClockStops group by {col}", cnxn)
        
    return(ppl)
    
    
def simple_sql_dt(dbconn, table, col, cnt, where, start, end):
    ''' extract data from sql'''
    where = where or ""
    if where and not where.lower().startswith("where"):
        where_clause = f"where {where}"
    
    with closing_connection(dbconn) as cnxn:
        out = pd.read_sql(f"select {col}, {cnt} as row_count from {table} {where} group by {col}", cnxn)
        
    suppressed = out.loc[out["row_count"]<=5]
    
    out = out.copy().loc[out["row_count"]>5]

    # round counts to nearest 10
    out["row_count"] = (10*((out["row_count"]/10).round(0))).astype(int)

    out[col] = pd.to_datetime(out[col],format='%Y-%m-%d',errors='coerce')
    out.sort_values(col,inplace=True)
    out = out[(out[col]>start) & (out[col]<end)]
    
    return out

def simple_sql_dt2(dbconn, table, col1, col2, cnt, where, start, end):
    ''' extract data from sql'''
    where = where or ""
    if where and not where.lower().startswith("where"):
        where_clause = f"where {where}"
    
    with closing_connection(dbconn) as cnxn:
        out = pd.read_sql(f"select {col1}, {col2}, {cnt} as row_count from {table} {where} group by {col1}, {col2}", cnxn)
        
        out[col1] = pd.to_datetime(out[col1],format='%Y-%m-%d',errors='coerce')
        out[col2] = pd.to_datetime(out[col2],format='%Y-%m-%d',errors='coerce')

        out.sort_values(col1,col2,inplace=True)
        
        out = out[(out[col1]>start) & (out[col1]<end)]

    return out

def by_year(dbconn, table, col, cnt, where):
    ''' extract data from sql'''
    where = where or ""
    if where and not where.lower().startswith("where"):
        where_clause = f"where {where}"
    
    with closing_connection(dbconn) as cnxn:
        out = pd.read_sql(f"select {col}, Week_Ending_Date, {cnt} as row_count from {table} {where} and Week_Ending_Date = '2022-05-01' group by Week_Ending_Date, {col}", cnxn)
        
        out[col] = pd.to_datetime(out[col],format='%Y-%m-%d',errors='coerce')

        out.sort_values(col,inplace=True)
        
        out['Year']=pd.DatetimeIndex(out[col]).year
        out = out[(out["Year"]>1998) & (out["Year"]<2023)]
        out = suppress_and_round2(out, "row_count", keep=False)
        out2 = out.groupby(['Year'])['row_count'].sum().reset_index(name = "n")
        
    return out2

def max_date(dbconn, table, where, start, end):
    ''' extract data from sql'''
    where = where or ""
    if where and not where.lower().startswith("where"):
        where_clause = f"where {where}"
    
    with closing_connection(dbconn) as cnxn:
        out = pd.read_sql(f"select distinct PSEUDO_PATIENT_PATHWAY_IDENTIFIER, max(Week_Ending_Date) as Max_Ending_Date from {table} group by PSEUDO_PATIENT_PATHWAY_IDENTIFIER", cnxn)
        
        out["Max_Ending_Date"] = pd.to_datetime(out["Max_Ending_Date"],format='%Y-%m-%d',errors='coerce')
        out.sort_values("Max_Ending_Date",inplace=True)
        
        out = out[(out["Max_Ending_Date"]>start) & (out["Max_Ending_Date"]<end)]
        
        out = out.groupby(['Max_Ending_Date'])['Max_Ending_Date'].count().reset_index(name='row_count')

    return out

def freq_dist(dbconn, table, col, where):
    where = where or ""
    if where and not where.lower().startswith("where"):
        where_clause = f"where {where}"
        
    with closing_connection(dbconn) as cnxn:
        out = pd.read_sql(f"select distinct {col}, count(*) as row_count from {table} group by {col}", cnxn)
        
    out = suppress_and_round2(out, field = "row_count", keep=False)
    out.sort_values("row_count", ascending=False, inplace=True)
    
    return(out)
        
            
        
                
def diff_date(dbconn, table, columns, threshold=1, where=None, include_counts=True):
    ''' Compare two columns (e.g ints, dates) based on their values
    Optionally filter using a where clause. 
    Row counts are rounded to nearest 5 and any values which appear <=5 times not shown.
    
    Inputs:
    dbconn (str): database connection details
    table (str): table name to query
    columns (list): name of 2 columns
    where (str): where clause e.g. "field_x in('value_1', 'value_2')"
    include_counts (bool): return list of fields without counts if False
    '''
    if len(columns)>2:
        print("Reduce number of columns to 2")
        return
    
    columns_str = ",".join(columns)
    
    where_clause = ""
        
    with closing_connection(dbconn) as cnxn:
        # extract all combinations of dates with counts of their occurrences
        out = pd.read_sql(f"select {columns_str}, count(*) as row_count from {table} {where_clause} group by {columns_str}", cnxn)
    
    a = columns[0]
    b = columns[1]
    
    out["difference"] = 0
    out.loc[out[a] < out[b], "comparison"] = f"{a} < {b}"
    out.loc[out[a] == out[b], "comparison"] = f"{a} = {b}"
    out.loc[out[a] > out[b], "comparison"] = f"{a} > {b}"
    out.loc[pd.isnull(out[a]), "comparison"] = f"{a} is missing"
    out.loc[pd.isnull(out[b]), "comparison"] = f"{b} is missing"
    
    days_flag=False
    if (out[a].dtype.kind in 'uUf') and (out[b].dtype.kind in 'uUf'):
        # for numeric dtypes:
        out.loc["difference"] = out[b]-out[a]
        # note this will also be valid for datetime types, will need fixing
    elif (out[a].dtype.kind in 'SOM') and (out[b].dtype.kind in 'SOM'): 
        # dates or date-like strings 
        try: 
            # coerce strings to dates
            out[a] = pd.to_datetime(out[a], errors="coerce")
            out[b] = pd.to_datetime(out[b], errors="coerce")

            out.loc[(pd.notnull(out[a])) & (pd.notnull(out[b])), "difference"] = (out[b]-out[a]).dt.days
            out["difference"] = out["difference"].astype(int)
            days_flag = True
        except:
            # if strings are not date-like
            display ("Check dtypes")
            return
    else: 
        display ("Check dtypes")
        return
    
    return(out)



def diff_date2(dbconn, table, columns, threshold=1, where=None, include_counts=True):
    ''' Compare two columns (e.g ints, dates) based on their values
    Optionally filter using a where clause. 
    Row counts are rounded to nearest 5 and any values which appear <=5 times not shown.
    
    Inputs:
    dbconn (str): database connection details
    table (str): table name to query
    columns (list): name of 2 columns
    where (str): where clause e.g. "field_x in('value_1', 'value_2')"
    include_counts (bool): return list of fields without counts if False
    '''
    if len(columns)>2:
        print("Reduce number of columns to 2")
        return
    
    columns_str = ",".join(columns)
    
    where_clause = ""
        
    with closing_connection(dbconn) as cnxn:
        # extract all combinations of dates with counts of their occurrences
        out = pd.read_sql(f"select {columns_str}, count(*) as row_count from {table} n where Week_Ending_Date=(select max(Week_Ending_Date) from {table} where Patient_ID =n.Patient_ID) group by {columns_str}", cnxn)
    
    a = columns[0]
    b = columns[1]
    
    out["difference"] = 0
    out.loc[out[a] < out[b], "comparison"] = f"{a} < {b}"
    out.loc[out[a] == out[b], "comparison"] = f"{a} = {b}"
    out.loc[out[a] > out[b], "comparison"] = f"{a} > {b}"
    out.loc[pd.isnull(out[a]), "comparison"] = f"{a} is missing"
    out.loc[pd.isnull(out[b]), "comparison"] = f"{b} is missing"
    
    days_flag=False
    if (out[a].dtype.kind in 'uUf') and (out[b].dtype.kind in 'uUf'):
        # for numeric dtypes:
        out.loc["difference"] = out[b]-out[a]
        # note this will also be valid for datetime types, will need fixing
    elif (out[a].dtype.kind in 'SOM') and (out[b].dtype.kind in 'SOM'): 
        # dates or date-like strings 
        try: 
            # coerce strings to dates
            out[a] = pd.to_datetime(out[a], errors="coerce")
            out[b] = pd.to_datetime(out[b], errors="coerce")

            out.loc[(pd.notnull(out[a])) & (pd.notnull(out[b])), "difference"] = (out[b]-out[a]).dt.days
            out["difference"] = out["difference"].astype(int)
            days_flag = True
        except:
            # if strings are not date-like
            display ("Check dtypes")
            return
    else: 
        display ("Check dtypes")
        return
    
    return(out)

def pct(col):
    display("10th percentile")
    print(np.percentile(col, 10, interpolation="midpoint"))
    display("25th percentile")
    print(np.percentile(col, 25, interpolation="midpoint"))
    display("median")
    print(np.percentile(col, 50, interpolation="midpoint"))
    display("75th percentile")
    print(np.percentile(col, 75, interpolation="midpoint"))
    display("90th percentile")
    print(np.percentile(col, 90, interpolation="midpoint"))
    display("95th percentile")
    print(np.percentile(col, 95, interpolation="midpoint"))
    display("Mean")
    print(np.mean(col))
    
def row_max_week(dbconn,col, table):
    with closing_connection(dbconn) as cnxn:
        # extract all combinations of dates with counts of their occurrences
        out = pd.read_sql(f"select  Week_Ending_Date, {col}, count(*) as row_count from {table} n where Week_Ending_Date=(select max(Week_Ending_Date) from {table} where Patient_ID =n.Patient_ID) group by Week_Ending_Date, {col}", cnxn)
        
        return(out)

def sample(dbconn,columns,table,where):    
    
    columns_str = ",".join(columns)
    
    with closing_connection(dbconn) as cnxn:
        samp = pd.read_sql(f"select {columns_str}, count(*) as row_count from {table} {where} \
                           group by {columns_str}", cnxn)
    return(samp)
        
    
def suppress_and_round2(df, field="row_count", keep=False):
    ''' In dataframe df with a row_count column, extract values with a row_count <=5 into a separate table, and round remaining values to neareast 10.
    Return df with low values suppressed and all remaining values rounded. Or if keep==True, retain the low value items in the table (but will appear with zero counts)
    '''
    # extract values with low counts into a seperate df
    suppressed = df.loc[df[field]<=5]
    if keep==False:
        df = df.copy().loc[df[field]>5]
    else:
        df = df.copy()
    # round counts to nearest 10
    df[field] = (10*((df[field]/10).round(0))).astype(int)
    return df