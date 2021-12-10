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
    n = Namespace(args.namespace)
    set_params(args.organization_name, source, n)

    hbase_table = f"{source}_buildings_{args.user}"
    hbase = happybase.Connection(**hbase_conn)
    t = hbase.table(hbase_table)
    print("getting hbase")
    data = list(t.scan())
    dic_list = []
    print("parsing hbase")
    for n_ens, x in data:
        x.update({"Num_Ens_Inventari": n_ens})
        dic_list.append(x)
    df = pd.DataFrame.from_records(dic_list)
    neo4j_connection = {"uri": config['neo4j']['uri'],
                        "auth": (config['neo4j']['username'], config['neo4j']['password'])}
    if args.organizations:
        g = generate_rdf(get_mappings("all"), df)

        # Get all existing Organizations
        neo = GraphDatabase.driver(**neo4j_connection)
        main_org_subject = n[slugify(args.organization_name)]
        with neo.session() as s:
            organization_names = s.run(f"""
            MATCH (m:ns0__Organization {{uri: "{main_org_subject}"}})-[*]->(n:ns0__Organization{{ns0__organizationDivisionType: "Department"}})
            RETURN n.uri
            """)
            dep_uri = [x.value() for x in organization_names]

        # Get all organizations in graph
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
    else:
        g = generate_rdf(get_mappings("buildings"), df)

    save_rdf_with_source(g, source, neo4j_connection)
