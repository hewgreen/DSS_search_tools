# Project metadata scraping

Quick crawling of DSS for projects. Uses the bundle endpoint of the DSS but avoids crawling through all bundles by submitting one ES query per dataset. Returns high level project metadata.

## To run

Clone this directory from the commandline to your local machine

`git clone https://github.com/hewgreen/DSS_search_tools`

Setup and initiate a virtual environment

`vitrualenv -p python3 venv`

`source venv/bin/activate`

Install requirments

`pip install -r requirements.txt`

Run example script wth no params

`python example.py`

This script shows you how the main function can be used. Metadata on either 'all' or 'unique by project name' are returned as dataframes.
