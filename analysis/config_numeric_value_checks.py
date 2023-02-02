
# enter codelist name as a string (include the full name of the csv, excluding the file extension)
# e.g. 'opensafely-creatinine'
# e.g. 'opensafely-ast'

codelist_name = "nhsd-primary-care-domain-refsets-bp_cod"


# specify headers for code column and term (description) column in dataset
# These should be strings
code_column = "code"
term_column = "term"

# enter year
# (must be a four-digit numeric value e.g. 2020)
year = 2022

# enter expected lower and upper bounds for range of values.
# Use positive integers
# Note zeros will be treated as missing.
# e.g. upper = 120
lower = 50
upper = 180


