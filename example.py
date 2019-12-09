__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "29/06/2019"


import DSS_search_tools

# datasetCrawl = DSS_search_tools.DSSdatasetDiscovery()
# project_list = datasetCrawl.unique_project_metadata_df # list of project uuids in DSS
# project_metadata = datasetCrawl.unique_project_metadata_df # metadata about the projects in the DSS


# schemaInfo = DSS_search_tools.schemaTools()
# latest_attributes = schemaInfo.latest_attributes # full list of the latest attributes from the schema


# limit_run = False # to search all bundles in project
limit_run = 10 # to only search a set amount of bundles per project
attribute_counting = DSS_search_tools.projectInspector(['008e40e8-66ae-43bb-951c-c073a2fa6774'], limit_run) # pass a list of project uuids here, these can be generated from functions above
attributes_per_project = attribute_counting.attributes_per_project # dictionary of attributes per project
attribute_counts_per_project = attribute_counting.attribute_counts # counts of attributes

print('Done')

