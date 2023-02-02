from cohortextractor import codelist_from_csv
from config import codelist_path

# Change the path of the codelist to your chosen codelist
codelist = codelist_from_csv(codelist_path, system="snomed")
