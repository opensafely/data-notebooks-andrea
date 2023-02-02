import pandas as pd
import numpy as np
import pyodbc
import os
from contextlib import contextmanager



# use this to open connection
@contextmanager
def closing_connection(dbconn): 
    cnxn = pyodbc.connect(dbconn)
    try: 
        yield cnxn 
    finally: 
        cnxn.close()
        
def suppress_and_round(df, field="row_count", keep=False):
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
    return df, suppressed


def simple_sql(dbconn, table, col, where):
    ''' extract data from sql'''
    where = where or ""
    if where and not where.lower().startswith("where"):
        where_clause = f"where {where}"
    
    with closing_connection(dbconn) as cnxn:
        out = pd.read_sql(f"select {col}, count(*) as row_count from {table} {where} group by {col}", cnxn)
    return out


def import_codelist(codelist_name, code_column='code', term_column='term'):
    ''' Import codelist_name.csv from the codelists folder. 
        code_column / term_column specify the names of the columns which are imported
    '''
    codelist = pd.read_csv(os.path.join('..','codelists', f'{codelist_name}.csv'), usecols=[code_column, term_column])
    codelist[code_column] = codelist[code_column].astype(str)
    return codelist