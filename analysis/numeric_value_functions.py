from utilities import closing_connection
import pandas as pd
import os
from IPython.display import display

def numeric_values_query(dbconn, codelist, lower, upper, year):
    ''' extract code counts from sql, split by Numeric value within/outside specified range or missing'''
    
    codelist_str = "','".join(codelist['code'])
    
    sql = f'''
    SELECT ConceptID, 
    CASE 
    WHEN NumericValue > 0 AND (NumericValue < {lower} OR NumericValue >= {upper} ) THEN 'Value outside range'
    WHEN NumericValue >= 1 AND NumericValue < {upper} THEN 'Value in range ({lower}-<{upper})' 
    ELSE 'No value' END AS Numeric_value_breakdown,
    COUNT (*) AS Total

     FROM CodedEvent_SNOMED
    WHERE ConceptID in (
    '{codelist_str}'
    )
    
    AND YEAR(ConsultationDate)={year}

    GROUP BY 
    ConceptID,
    CASE 
    WHEN NumericValue > 0 AND (NumericValue < {lower} OR NumericValue >= {upper} ) THEN 'Value outside range'
    WHEN NumericValue >= 1 AND NumericValue < {upper} THEN 'Value in range ({lower}-<{upper})' 
    ELSE 'No value' END
    order by conceptID, Numeric_value_breakdown
    '''

    with closing_connection(dbconn) as cnxn:
        out = pd.read_sql(sql, cnxn)
    
    print("Data imported")
    return out


def summarise_numeric_values(df, codelist_name, codelist, code_column, term_column):
    df1 = df.copy()
    df1 = df1.merge(codelist, left_on= 'ConceptID', right_on=code_column, how="outer").drop(columns=["ConceptID"])
    df1["Numeric_value_breakdown"] = df1["Numeric_value_breakdown"].fillna("NA")

    # calculate totals and percentages (and convert to thousands)
    df1 = df1.groupby([code_column, term_column, "Numeric_value_breakdown"]).sum()
    df1["Test_total"] = (df1.groupby([code_column, term_column])["Total"].sum()/1000).round(1)
    df1["Total"] = (df1["Total"]/1000).round(1)
    df1["Percent"] = (100*(df1["Total"]/df1["Test_total"]).round(3)).fillna(0)

    # rename and sort
    df1 = df1.rename(columns={"Test_total":"Total test count (thousands)", "Total":"Value count (thousands)"})
    df1 = df1.sort_values(by="Total test count (thousands)", ascending=False)

    #export to csv
    df1.to_csv(os.path.join('..','output', f'value_counts_{codelist_name}.csv'))

    display (df1)