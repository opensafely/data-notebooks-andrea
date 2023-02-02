import nbformat as nbf
from config_numeric_value_checks import codelist_name


nb = nbf.v4.new_notebook()


imports = """
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()
from IPython.display import HTML
from IPython.display import Markdown as md
from IPython.core.display import HTML as Center

from IPython.display import Image, display
%matplotlib inline

import pyodbc
import os
from datetime import date, datetime

sys.path.append('../analysis/')
from utilities import import_codelist
from numeric_value_functions import *
from config_numeric_value_checks import *

pd.set_option('display.max_colwidth', 250)
pd.set_option('display.max_rows', 100)

# get the server credentials
dbconn = os.environ.get('FULL_DATABASE_URL', None).strip('"')

"""

header = """
display(
md(f"# Data Plausibility Check (numeric values) for *{codelist_name}* codelist"),
md(f"Note: all row/patient counts are shown in thousands."),
)
"""

methods = """
display(
md("### Methods"),
md(f"This includes all patients in OpenSAFELY-TPP, covering 40% of England's population."),
md(f"All analytical code and output is available for inspection at the [OpenSAFELY GitHub repository](https://github.com/opensafely)")
)
"""

notebook_run_date = """\
display(
md(f'''This notebook was run on {date.today().strftime('%Y-%m-%d')}
    and includes all activity within the *{codelist_name}* codelist recorded in {year}.'''
    )
)"""

get_data = """
image_paths['total'] = '../output/joined/plot_total.png'
"""
#image_paths = {d: f'../output/joined/plot_{d}.png' for d in demographics}

get_codelist = """
display(md("### Codelist"))
codelist = import_codelist(codelist_name=codelist_name, code_column=code_column, term_column=term_column)
display(codelist)
"""

get_data = """
display(md("### Import data"))
df = numeric_values_query(dbconn, codelist=codelist, lower=lower, upper=upper, year=year)
"""

output = """
display(md("### Numeric Value Summary"))
summarise_numeric_values(df=df, codelist_name=codelist_name, codelist=codelist, code_column=code_column, term_column=term_column)
"""

nb['cells'] = [
    nbf.v4.new_code_cell(imports),
    nbf.v4.new_code_cell(header),
    nbf.v4.new_code_cell(methods),
    nbf.v4.new_code_cell(notebook_run_date),
    nbf.v4.new_code_cell(get_codelist),
    nbf.v4.new_code_cell(get_data),
    nbf.v4.new_code_cell(output),
    ]

counter = """\
i=0
"""

# nb['cells'].append(nbf.v4.new_code_cell(counter))

# for d in range(len(demographics)):
#     cell_counts = """\
#     display(
#     md(f"## Breakdown by {demographics[i]}")
#     )
   
#     """
#     nb['cells'].append(nbf.v4.new_code_cell(cell_counts))
    
#     cell_plot = """\
#     display(Image(filename=image_paths[demographics[i]]))
#     i+=1
#     """
#     nb['cells'].append(nbf.v4.new_code_cell(cell_plot))


nbf.write(nb, f'notebooks/Notebook_numeric_values_{codelist_name}.ipynb')
