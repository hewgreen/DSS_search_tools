from DSS_search_tools import find_unique_project_uuids

projects = find_unique_project_uuids()
project_list = projects[0]
projects_found = projects[1]
unique_projects_found = projects[1]

print(unique_projects_found.to_csv('temp.csv'))

print(unique_projects_found['provenance.document_id'].tolist())

