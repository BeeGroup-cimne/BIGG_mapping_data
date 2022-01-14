import json
import os
import argparse
from functools import partial

import pandas as pd
from neo4j import GraphDatabase
from slugify import slugify

from utils import encrypt

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create Gemweb connections.')
    main_org_params = parser.add_argument_group("Organization",
                                                "Set the main organization information for importing the data")
    main_org_params.add_argument("--organization_name", "-name", help="The main organization name", required=True)
    main_org_params.add_argument("-f", "--file", help="Import the organization from file", required=True)
    main_org_params.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    main_org_params.add_argument("--datasource", "-d", help="The datasource to create credentials", required=True)

    with open("./config.json") as config_f:
        config = json.load(config_f)
        config['neo4j']['auth'] = tuple(config['neo4j']['auth'])

    if os.getenv("PYCHARM_HOSTED"):
        args_t = ["-f", "DataSources/data/gemweb.xls", "-name", "Generalitat de Catalunya", "-n", "http://icaen.cat#", "-d", "GemwebSource"]
        args = parser.parse_args(args_t)
    else:
        args = parser.parse_args()

    neo = GraphDatabase.driver(**config['neo4j'])
    df = pd.read_excel(args.file)
    df['Password_enc'] = df['Password'].apply(partial(encrypt, password=os.getenv(config['encript_pass']['environment'])))
    with neo.session() as s:
        for _, org in df.iterrows():
            res = s.run(
                f"""
                    MATCH (o:ns0__Organization {{uri:"{args.namespace}{slugify(org['Organization'])}"}})
                    Merge (x:{args.datasource} {{username:"{org.Username}"}})<-[:ns0__hasSource]-(o)
                    SET x.password="{org.Password_enc}"
                    RETURN x
                """)


