import argparse
import json
import re

from neo4j import GraphDatabase
from rdflib import Namespace
import happybase
import pandas as pd
import os

import settings
from EEM.EEM_mapping import set_params, get_mappings
from EEM.mapper import map_data
from EEM.transform_functions import get_code_ens
from rdf_utils.rdf_functions import generate_rdf
from utils import save_rdf_with_source, decode_hbase, id_zfill, get_hbase_data_batch, read_config

from fuzzywuzzy import process

source = "genercat"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Mapping of EEM data to neo4j.')
    main_org_params = parser.add_argument_group("Organization",
                                                "Set the main organization information for importing the data")
    main_org_params.add_argument("--user", "-u", help="The main organization name", required=True)
    main_org_params.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)

    if os.getenv("PYCHARM_HOSTED"):
        args_t = ["-n", "http://icaen.cat#", "-u", "icaen" ]
        args = parser.parse_args(args_t)
    else:
        args = parser.parse_args()
    # read config file
    config = read_config(settings.conf_file)


    hbase_conn = config['hbase_imported_data']

    hbase_table = f"{source}_eem_{args.user}"
    hbase = happybase.Connection(**hbase_conn)
    print("getting hbase")
    for data in get_hbase_data_batch(hbase_conn, hbase_table, batch_size=100000):
        dic_list = []
        print("parsing hbase")
        for id_, x in data:
            item = dict()
            for k, v in x.items():
                k1 = re.sub("^info:", "", k.decode())
                item[k1] = v
            item.update({"id_": id_})
            dic_list.append(item)
        map_data(dic_list, config=config, source=source, namespace=args.namespace,
                 user=args.user)
