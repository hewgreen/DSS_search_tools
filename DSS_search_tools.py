from hca.dss import DSSClient
import pandas as pd

def find_unique_project_uuids():

    def get_dss_generator(query):
        dss_client = DSSClient(swagger_url="https://dss.data.humancellatlas.org/v1/swagger.json")
        bundle_generator = dss_client.post_search.iterate(replica="aws", es_query=query, output_format="raw")
        total_hits = dss_client.post_search(replica="aws", es_query=query, output_format="raw").get('total_hits')
        return (bundle_generator, total_hits)

    def append_project_list_to_query(query, unique_projects):
        query.get("query").get("bool").get("must_not")[0].get("terms")[
            "files.project_json.provenance.document_id"] = unique_projects
        return query

    def find_unique_project(initial_q, unique_projects, metadata):
        query = append_project_list_to_query(initial_q, unique_projects)

        get_generator = get_dss_generator(query)
        bundle_generator = get_generator[0]
        total_hits = get_generator[1]

        if total_hits == 0:
            return (unique_projects, False, metadata)

        for bundle in bundle_generator:
            data = bundle.get('metadata')
            project_uuid = data.get('files').get('project_json')[0].get('provenance').get('document_id')
            if project_uuid not in unique_projects:
                unique_projects.append(project_uuid)
                metadata.append(data.get('files').get('project_json')[0])
                return (unique_projects, True, metadata)

    initial_q = {
                "query": {
                    "bool": {
                        "must_not": [
                            {
                                "terms": {
                                    "files.project_json.provenance.document_id": []
                                }

                            }
                        ],
                        "must": [
                            {
                                "exists": {
                                    "field": "files.project_json.provenance.document_id"
                                }
                            }
                        ]
                    }
                }
            }


    unique_projects = []
    metadata = []
    found_new_project = True
    while found_new_project:
        progress = find_unique_project(initial_q, unique_projects, metadata)
        found_new_project = progress[1]
        unique_projects = progress[0]
        metadata = progress[2]
        print("Found {} projects".format(len(unique_projects)))

    summary = metadata_flattener(metadata)
    return (unique_projects, summary[0], summary[1])


def metadata_flattener(metadata): # this is metadata returned from project_json no nested metadata is returned.

    project_attributes = []

    for project in metadata:
        project_flat_meta = {}
        for key, value in project.items():
            if type(value) in [str, int]:
                project_flat_meta[key] = value
            elif type(value) == list:
                if len(value) > 0:
                    if type(value[0]) in [str, int]:
                        project_flat_meta[key] = str(value)
            elif type(value) == dict:
                for nested_key, nested_value in value.items():
                    if type(nested_value) in [str, int]:
                        project_flat_meta[key + '.' + nested_key] = nested_value
                    elif type(nested_value) == list:
                        if len(nested_value) > 0:
                            if type(nested_value[0]) in [str, int]:
                                project_flat_meta[key + '.' + nested_key] = str(nested_value)
        project_attributes.append(project_flat_meta)

    project_title = 'project_core.project_title'
    update_date = 'provenance.submission_date'
    df = pd.DataFrame(project_attributes).sort_values([project_title, update_date])
    cols = list(df.columns)
    cols.insert(0, cols.pop(cols.index(project_title)))
    df = df[cols]

    unique = df.drop_duplicates(subset=[project_title], keep="last")
    print('Found {} unique projects'.format(unique.shape[0]))

    return (df, unique) # unique is a df with drop duplicates by project title





