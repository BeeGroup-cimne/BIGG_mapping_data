import argparse
import json

from neo4j import GraphDatabase
from rdflib import Namespace
import happybase
import pandas as pd

from EEM.EEM_mapping import set_params, get_mappings
from EEM.transform_functions import get_code_ens
from rdf_utils.rdf_functions import generate_rdf
from utils import save_rdf_with_source, decode_hbase, id_zfill
from fuzzywuzzy import process

source = "genercat"
#args_t = ["-name", "Generalitat de Catalunya", "-n", "http://data.icaen.cat#", "-u", "icaen" ]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Mapping of EEM data to neo4j.')
    main_org_params = parser.add_argument_group("Organization",
                                                "Set the main organization information for importing the data")
    main_org_params.add_argument("--organization_name", "-name", help="The main organization name", required=True)
    main_org_params.add_argument("--user", "-u", help="The main organization name", required=True)
    main_org_params.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)

    args = parser.parse_args()
    # read config file
    with open("./config.json") as config_f:
        config = json.load(config_f)
    hbase_conn = config['hbase']
    n = Namespace(args.namespace)
    set_params(args.organization_name, source, n)

    hbase_table = f"{source}_eem_{args.user}"
    hbase = happybase.Connection(**hbase_conn)
    t = hbase.table(hbase_table)
    print("getting hbase")
    data = list(t.scan())
    dic_list = []
    print("parsing hbase")
    for id_, x in data:
        x.update({"id_": id_})
        dic_list.append(x)
    df = pd.DataFrame.from_records(dic_list)
    df['ce'] = df[b'info:building_CodeEns_GPG'].apply(decode_hbase)
    df['ce'] = df['ce'].apply(get_code_ens)
    df['ce'] = df['ce'].apply(id_zfill)
    # upload only mapped measures
    df = df[df['ce'] != '00000']
    # Get all existing BuildingConstructionElements
    neo4j_connection = {"uri": config['neo4j']['uri'],
                        "auth": (config['neo4j']['username'], config['neo4j']['password'])}
    neo = GraphDatabase.driver(**neo4j_connection)
    with neo.session() as s:
        element_id = s.run("""
                    MATCH (m:ns0__Organization {uri: "http://data.icaen.cat#generalitat-de-catalunya"})-[*]->(n:ns0__BuildingConstructionElement {ns0__buildingConstructionElementType: 'Building'})
                    RETURN n.uri
                    """)
        uri = [get_code_ens(x.value()) for x in element_id]
    df = df[df["ce"].isin(uri)]
    g = generate_rdf(get_mappings("all"), df)
    save_rdf_with_source(g, source, neo4j_connection)
