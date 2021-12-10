import argparse
import json
from rdflib import Namespace, Graph
from Organization.organization_mapping import *
import pandas as pd

from rdf_utils.bigg_definition import Bigg
from rdf_utils.rdf_functions import generate_rdf
from utils import save_rdf_with_source

source = "Org"
#args_t = ["-f", "Organization/data/organizations.xls", "-name", "Generalitat de Catalunya", "-n", "http://data.icaen.cat#", "-u", "icaen"]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Mapping of Organization data to neo4j.')
    exec_settings = parser.add_argument_group("General", "General settings of the script")
    exec_settings.add_argument("-f", "--file", help="Import the organization from file", required=True)

    main_org_params = parser.add_argument_group("Organization",
                                                "Set the main organization information for importing the data")
    main_org_params.add_argument("--organization_name", "-name", help="The main organization name", required=True)
    main_org_params.add_argument("--user", "-u", help="The main organization name", required=True)
    main_org_params.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)

    args = parser.parse_args()
    # read config file
    with open("./config.json") as config_f:
        config = json.load(config_f)

    org_levels_df = []
    level = 0
    while True:
        try:
            org_levels_df.append(pd.read_excel(args.file, sheet_name=level))
            level += 1
        except IndexError:
            break

    n = Namespace(args.namespace)
    set_params(args.organization_name, source, n)

    print("mapping data")
    # generate main org
    g = generate_rdf(get_mappings("main"), pd.DataFrame())

    g_levels = []
    for dfl in org_levels_df:
        g_levels.append(generate_rdf(get_mappings("level"), dfl))

    # Create links between graph
    total_g = Graph()
    total_g += g
    for index, dfs in enumerate(org_levels_df):
        if index == 0:
            parent_df = None
            parent = args.organization_name
        else:
            parent = None
            parent_df = org_levels_df[index - 1]
        total_g += g_levels[index]
        for x, row in dfs.iterrows():
            if parent:
                total_g.add((n[slugify(parent)], Bigg.hasSubOrganization, n[slugify(row["name"])]))
            else:
                r_parent = parent_df[parent_df.id == row.link]
                total_g.add((n[slugify(r_parent['name'].values[0])], Bigg.hasSubOrganization, n[slugify(row["name"])]))

    neo4j_connection = {"uri": config['neo4j']['uri'], "auth": (config['neo4j']['username'], config['neo4j']['password'])}
    print("saving to node4j")
    save_rdf_with_source(total_g, source, neo4j_connection)
