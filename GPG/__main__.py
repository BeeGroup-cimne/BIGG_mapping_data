import argparse
import json
import os
import neo4j
from rdflib import Namespace
from GPG.GPG_mapping import *
import happybase
import pandas as pd
from rdf_utils.rdf_functions import generate_rdf
from utils import save_rdf_with_source
from fuzzywuzzy import process
source = "GPG"


def harmonize_organization_names(g, user_id, organization_name, namespace, neo4j_conn):
    # Get all existing Organizations typed department
    neo = GraphDatabase.driver(**neo4j_conn)
    with neo.session() as s:
        organization_names = s.run(f"""
         MATCH (m:ns0__Organization {{ns0__userId: "{user_id}"}})-[*]->(n:ns0__Organization{{ns0__organizationDivisionType: "Department"}})
         RETURN n.uri
         """)
        dep_uri = [x.value() for x in organization_names]

    # Get all organizations in rdf graph using sparql
    query_department = """
        PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX bigg:<http://example/BiggOntology#>
        SELECT DISTINCT ?sub
        WHERE {
            ?sub rdf:type bigg:Organization .
            ?sub bigg:organizationDivisionType "Department" .
        }
     """
    r_dep = g.query(query_department)
    g_altres = generate_rdf(get_mappings("other"), pd.DataFrame())
    g += g_altres
    altres_subj = list(set(g_altres.subjects()))[0]
    main_org_subject = namespace[slugify(organization_name)]
    g.add((rdflib.URIRef(main_org_subject), Bigg.hasSubOrganization, altres_subj))
    for dep_org in r_dep:
        query = str(dep_org[0])
        choices = dep_uri
        # Get a list of matches ordered by score, default limit to 5
        match, score = process.extractOne(query, choices)
        if score > 90:
            g2 = Graph()
            for s, p, o in g.triples((dep_org[0], None, None)):
                g2.add((rdflib.URIRef(match), p, o))
            g.remove((dep_org[0], None, None))
            g += g2
        else:
            g2 = Graph()
            for s, p, o in g.triples((dep_org[0], None, None)):
                g2.add((altres_subj, p, o))
            g.remove((dep_org[0], None, None))
            g += g2
    return g


def map_data(data, organization_name, namespace, user, organizations=False):

    with open("./config.json") as config_f:
        config = json.load(config_f)
    neo4j_conn = config['neo4j']
    neo4j_connection = {"uri": neo4j_conn['uri'],
                        "auth": (neo4j_conn['username'], neo4j_conn['password'])}
    n = Namespace(namespace)
    set_params(organization_name, source, n)
    df = pd.DataFrame.from_records(data)
    if organizations:
        g = generate_rdf(get_mappings("all"), df)
        g = harmonize_organization_names(g, user, organization_name, n, neo4j_connection)
    else:
        g = generate_rdf(get_mappings("buildings"), df)
    save_rdf_with_source(g, source, neo4j_connection)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Mapping of GPG data to neo4j.')
    exec_settings = parser.add_argument_group("General", "General settings of the script")
    exec_settings.add_argument("-o", "--organizations", action='store_true', help="Import the organization structure")

    main_org_params = parser.add_argument_group("Organization",
                                                "Set the main organization information for importing the data")
    main_org_params.add_argument("--organization_name", "-name", help="The main organization name", required=True)
    main_org_params.add_argument("--user", "-u", help="The main organization name", required=True)
    main_org_params.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    if os.getenv("PYCHARM_HOSTED"):
        args_t = ["-name", "Generalitat de Catalunya", "-n", "http://icaen.cat#", "-u", "icaen", "-o"]
        args = parser.parse_args(args_t)
    else:
        args = parser.parse_args()
    # read config file
    with open("./config.json") as config_f:
        config = json.load(config_f)
    hbase_conn = config['hbase']
    hbase_table = f"{source}_buildings_{args.user}"
    for data in get_hbase_data_batch(hbase_conn, hbase_table):
        dic_list = []
        print("parsing hbase")
        for n_ens, x in data:
            item = dict()
            for k, v in x.items():
                k1 = re.sub("^info:", "", k.decode())
                item[k1] = v
            item.update({"Num_Ens_Inventari": n_ens})
            dic_list.append(item)
        print("parsed. Mapping...")

        map_data(dic_list, args.organization_name, args.namespace, args.user, args.organizations)
