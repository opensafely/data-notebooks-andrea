# enter table name
# tables = ["WL_OpenPathways", "WL_ClockStops", "WL_Diagnostics"]



# enter filters for schema
# these are useful to (a) make the data size manageable (e.g. one month or year of data)
# and (b) look at a subset of interest e.g. patients with COVID
schema_filter = "WHERE Week_Ending_Date IS NOT NULL"

# columns to describe
# this produces a simple summary of the columns supplied
tables_to_describe = {
    "tables": {
        "WL_ClockStops": {"columns": "all"},
        "WL_Diagnostics": {"columns": "all"},
        "WL_OpenPathways": {"columns": "all"},
        },
    "threshold": 50,
    "where": schema_filter, 
    "include_counts": False,
    }

# check for duplicates -  IF NOT REQUIRED use empty dict
# columns here normally patient_id only;
# each column supplied will be checked for duplicates in isolation, not in combination
duplicates = {
    "columns": ["Patient_ID"], 
    "threshold": 50, 
    "where": schema_filter
}

