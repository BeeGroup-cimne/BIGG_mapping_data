import argparse
import json
import os
import re

import settings
from GPG.mapper_static import map_data
from utils import get_hbase_data_batch, read_config

source = "GPG"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Mapping of GPG data to neo4j.')
    exec_settings = parser.add_argument_group("General", "General settings of the script")
    exec_settings.add_argument("-o", "--organizations", action='store_true', help="Import the organization structure")

    main_org_params = parser.add_argument_group("Organization",
                                                "Set the main organization information for importing the data")
    main_org_params.add_argument("--user", "-u", help="The user importing the data", required=True)
    main_org_params.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    if os.getenv("PYCHARM_HOSTED"):
        args_t = ["-name", "Generalitat de Catalunya", "-n", "http://icaen.cat#", "-u", "icaen", "-o"]
        args = parser.parse_args(args_t)
    else:
        args = parser.parse_args()
    # read config file
    config = read_config(settings.conf_file)


    hbase_conn = config['hbase_imported_data']
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
        map_data(dic_list, namespace=args.namespace, user=args.user,
                 organizations=args.organizations, config=config)
