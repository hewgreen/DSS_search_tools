__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "29/06/2019"

from hca.dss import DSSClient
import pandas as pd
from ingest.template.schema_template import SchemaTemplate
from tqdm import tqdm
import sys
from collections import defaultdict

INITIAL_Q = {
    "query": {
        "bool": {
            "must": [
                {
                    "terms": {
                        "files.project_json.provenance.document_id": []
                    }

                }
            ]
        }
    }
}

def get_dss_generator(query):
    dss_client = DSSClient(swagger_url="https://dss.data.humancellatlas.org/v1/swagger.json")
    bundle_generator = dss_client.post_search.iterate(replica="aws", es_query=query, output_format="raw")
    total_hits = dss_client.post_search(replica="aws", es_query=query, output_format="raw").get('total_hits')
    return (bundle_generator, total_hits)

def append_project_list_to_query(query, unique_projects):
    query.get("query").get("bool").get("must")[0].get("terms")[
        "files.project_json.provenance.document_id"] = unique_projects
    return query

class DSSdatasetDiscovery:

    def __init__(self):

        projects = self.find_unique_project_uuids()
        self.unique_project_uuids = projects[0]
        self.unique_project_metadata_df = projects[1]

    def find_unique_project_uuids(self):

        def find_unique_project(initial_q, unique_projects, metadata):
            # method should be replaced by quicker query service when it is released
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

        print('Crawling DSS for unique projects')

        unique_projects = []
        metadata = []
        found_new_project = True
        while found_new_project:
            progress = find_unique_project(INITIAL_Q, unique_projects, metadata)
            found_new_project = progress[1]
            unique_projects = progress[0]
            metadata = progress[2]
            print("Found {} projects".format(len(unique_projects)))

        summary = self.metadata_flattener(metadata)
        return (unique_projects, summary[0], summary[1])

    def metadata_flattener(self, metadata):  # this is metadata returned from project_json no nested metadata is returned.

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

        return (df, unique)  # unique is a df with drop duplicates by project title

class schemaTools:
    def __init__(self):

        self.latest_attributes = self.list_of_latest_attributes()

    def list_of_latest_attributes(self):
        template = SchemaTemplate()

        latest_attributes = []
        for tab in template.tabs:
            for type, content in tab.items():
                attributes = content.get('columns')
                for attribute in attributes:
                    latest_attributes.append(type + '.' + attribute)
        return latest_attributes


class projectInspector:
    # pass this method a list of project uuids

    def __init__(self, project_uuids, limit_run):

        self.attributes_per_project = {}
        for project_uuid in project_uuids:
            query = append_project_list_to_query(INITIAL_Q, [project_uuid])
            get_generator = get_dss_generator(query)
            found_attributes = self.metadata_attribute_search(get_generator, limit_run)
            self.attributes_per_project[project_uuid] = found_attributes[1]

        self.attribute_counts = self.attribute_counts()

    def attribute_counts(self):
        assert len(self.attributes_per_project) != 0, "Didn't find and projects"
        counts = defaultdict(int)
        for project_uuid, attributes in self.attributes_per_project.items():
            for attribute in attributes:
                counts[attribute] += 1
        return counts


    def metadata_attribute_search(self, get_generator, limit_run):
        bundle_generator = get_generator[0]
        total_hits = get_generator[1]

        bundle_attributes = []
        temp_counter = 0

        for bundle in tqdm(bundle_generator, total=total_hits, unit='bundle'):
            bundle_meta = bundle.get('metadata').get('files')
            top_level = list(bundle_meta.keys())

            if len(top_level) == 0:
                print('WARN: Bundle had empty metadata')  # check for erronious bundles
                sys.exit()

            detected_attributes = []

            # certain branches can be ignored when exploring the tree.
            ignore_top_level = ['links_json', 'analysis_process']
            ignore_mid_level = ['schema_type', 'provenance', 'describedBy']

            for x in top_level:
                if x in ignore_top_level:  # skip these fields
                    continue
                for meta_doc in bundle_meta.get(x):
                    top_level = x[:-5]  # strip '_json'
                    for mid_level, value in meta_doc.items():
                        if mid_level in ignore_mid_level:
                            continue
                        if type(value) != list:
                            value_list = [value]
                        else:
                            value_list = value
                        for item in value_list:
                            if isinstance(item, (str, int, bool, float)):
                                attribute = '.'.join([top_level, mid_level])
                                detected_attributes.append(attribute)
                                continue
                            for low_level in item.items():
                                if low_level in ignore_mid_level:
                                    continue
                                attribute = '.'.join([top_level, mid_level, low_level[0]])
                                detected_attributes.append(attribute)

            bundle_attributes.append(detected_attributes)

            temp_counter += 1  # remove for full run
            if temp_counter == limit_run:
                break

        print('{} of {} bundles processed'.format(len(bundle_attributes), total_hits))
        project_attribute_list = set([item for sublist in bundle_attributes for item in sublist])
        return detected_attributes, project_attribute_list










