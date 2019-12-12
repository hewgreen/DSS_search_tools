__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "29/06/2019"


import DSS_search_tools
import json

project_list = [
'e0009214-c0a0-4a7b-96e2-d6a83e966ce0',
'ae71be1d-ddd8-4feb-9bed-24c3ddb6e1ad',
'74b6d569-3b11-42ef-b6b1-a0454522b4a0',
'2043c65a-1cf8-4828-a656-9e247d4e64f1',
'cddab57b-6868-4be4-806f-395ed9dd635a',
'1defdada-a365-44ad-9b29-443b06bd11d6',
'88ec040b-8705-4f77-8f41-f81e57632f7d',
'8c3c290d-dfff-4553-8868-54ce45f4ba7f',
'f83165c5-e2ea-4d15-a5cf-33f3550bffde',
'8185730f-4113-40d3-9cc3-929271784c2b',
'cc95ff89-2e68-4a08-a234-480eca21ce79',
'a9c022b4-c771-4468-b769-cabcf9738de3',
'abe1a013-af7a-45ed-8c26-f3793c24a1f4',
'f86f1ab4-1fbb-4510-ae35-3ffd752d4dfc',
'4d6f6c96-2a83-43d8-8fe1-0f53bffd4674',
'005d611a-14d5-4fbf-846e-571a1f874f70',
'c4077b3c-5c98-4d26-a614-246d12c2e5d7',
'577c946d-6de5-4b55-a854-cd3fde40bff2',
'f8aa201c-4ff1-45a4-890e-840d63459ca2',
'4e6f083b-5b9a-4393-9890-2a83da8188f1',
'116965f3-f094-4769-9d28-ae675c1b569c',
'027c51c6-0719-469f-a7f5-640fe57cbece',
'008e40e8-66ae-43bb-951c-c073a2fa6774',
'9c20a245-f2c0-43ae-82c9-2232ec6b594f',
'f81efc03-9f56-4354-aabb-6ce819c3d414',
'091cf39b-01bc-42e5-9437-f419a66c8a45',
'90bd6933-40c0-48d4-8d76-778c103bf545',
'4a95101c-9ffc-4f30-a809-f04518a23803']

# Internal server error 'a29952d9-925e-40f4-8a1c-274f118f1f51',

# datasetCrawl = DSS_search_tools.DSSdatasetDiscovery()
# project_list = datasetCrawl.unique_project_metadata_df # list of project uuids in DSS
# project_metadata = datasetCrawl.unique_project_metadata_df # metadata about the projects in the DSS


# schemaInfo = DSS_search_tools.schemaTools()
# latest_attributes = schemaInfo.latest_attributes # full list of the latest attributes from the schema


# limit_run = False # to search all bundles in project
limit_run = 10 # to only search a set amount of bundles per project
attribute_counting = DSS_search_tools.projectInspector(project_list, limit_run) # pass a list of project uuids here, these can be generated from functions above
attributes_per_project = attribute_counting.attributes_per_project # dictionary of attributes per project
attribute_counts_per_project = attribute_counting.attribute_counts # counts of attributes

with open('attribute_counts_per_project_data.txt', 'w') as outfile:
    json.dump(attribute_counts_per_project, outfile)
with open('attributes_per_project_data.txt', 'w') as outfile:
    attributes_per_project = {k:list(v) for k , v in attributes_per_project.items()}
    json.dump(attributes_per_project, outfile)


print('Done')

