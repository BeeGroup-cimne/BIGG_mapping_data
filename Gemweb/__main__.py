import argparse
import hashlib
import json
import re
import urllib

import happybase
import pandas as pd
from rdflib import Namespace
from Gemweb.Gemweb_mapping import *
from rdf_utils.rdf_functions import generate_rdf
from utils import save_rdf_with_source


def static_mapping(args, source, hbase_conn, neo4j_connection):
    # get supplies from HBASE
    supplies_table = f"{source}_supplies_{args.user}".lower()
    hbase = happybase.Connection(**hbase_conn)
    hbase_supplies_table = hbase.table(supplies_table)
    HB_supplies = list(hbase_supplies_table.scan())
    supplies_list = []
    for gem_id, data in HB_supplies:
        data.update({"dev_gem_id": gem_id})
        supplies_list.append(data)
    supplies_df = pd.DataFrame.from_records(supplies_list)

    # get buildings from HBASE
    hbase = happybase.Connection(**hbase_conn)
    building_table = f"{source}_buildings_{args.user}".lower()
    hbase_building_table = hbase.table(building_table)
    HB_buildings = list(hbase_building_table.scan())
    building_list = []
    for gem_id, data in HB_buildings:
        data.update({"build_gem_id": gem_id})
        building_list.append(data)
    building_df = pd.DataFrame.from_records(building_list)
    building_df.set_index("build_gem_id", inplace=True)

    # Join dataframes by link
    df = supplies_df.join(building_df, on=b'info:id_centres_consum', lsuffix="supply", rsuffix="building")

    neo = GraphDatabase.driver(**neo4j_connection)
    ids_ens = []
    with neo.session() as ses:
        buildings_neo = ses.run("Match (n: ns0__Building) return n.uri")
        ids_ens = list(set([urlparse(x.get("n.uri")).fragment.split("-")[1] for x in buildings_neo]))
    # create num_ens column with parsed values in df
    df['num_ens'] = df[b'info:codi'].apply(lambda x: decode_hbase(x).zfill(5))

    # get all devices with linked buildings
    df_linked = df[df['num_ens'].isin([str(i) for i in ids_ens])]

    g = generate_rdf(get_mappings("linked"), df_linked)
    save_rdf_with_source(g, source, neo4j_connection)

    df_unlinked = df[df['num_ens'].isin([str(i) for i in ids_ens]) == False]
    g2 = generate_rdf(get_mappings("unlinked"), df_unlinked)
    save_rdf_with_source(g2, source, neo4j_connection)


def time_series_mapping(args, source, hbase_conf, hbase_conf2, neo4j_connection):
    neo = GraphDatabase.driver(**neo4j_connection)

    with neo.session() as session:
        devices_neo = session.run("""
        MATCH (n:ns0__Organization {uri:"http://data.icaen.cat#generalitat-de-catalunya"})-[:ns0__hasSource]-
        (g:ns0__GemwebSource)<-[:ns0__importedFromSource]-(d) 
        RETURN d""")
        for d_neo in devices_neo:
            url = d_neo['d']['uri']
            parsed = urllib.parse.urlparse(url)
            device_id = parsed.fragment.split("-")[0]

            hbase = happybase.Connection(**hbase_conf)
            ts_tables = [x for x in hbase.tables() if re.match(rf"{source}_data_.*", x.decode())]
            for htable_name in ts_tables:
                freq = htable_name.decode().split("_")[2]
                user = htable_name.decode().split("_")[3]
                prefix = (device_id+'~').encode("utf-8")
                list_id = f"{device_id}-LIST-RAW-{freq}-{source}"
                list_uri = f"{parsed.scheme}://{parsed.netloc}/#{list_id}"
                new_d_id = hashlib.sha256(parsed.fragment.encode("utf-8"))
                new_d_id = new_d_id.hexdigest()
                for data in get_hbase_data_batch(hbase_conf, htable_name, batch_size=100000, row_prefix=prefix):
                    if not data:
                        break
                    session.run(f"""
                    MATCH (device: ns0__Device {{uri:"{url}"}})
                    MERGE (list: ns0__MeasurementList {{
                        uri: "{list_uri}",
                        listID: "{new_d_id}",
                        measurementUnit: "kWh",
                        measurementReadingType: "estimated",
                        measuredProperty: "{d_neo['d']['ns0__deviceType']}"
                    }})<-[:hasMeasurementLists]-(device) return list
                    """)

                    hbase2 = happybase.Connection(**hbase_conf2)
                    device_table = f"data_{freq}_{user}_device"
                    try:
                        hbase2.create_table(device_table, {"info": {}, "v": {}})
                    except:
                        pass

                    hbase2 = happybase.Connection(**hbase_conf2)
                    ts_device = hbase2.table(device_table)
                    batch = ts_device.batch()
                    for key, value in data:
                        _, timestamp = key.decode().split("~")
                        key = f'{new_d_id}~{timestamp}'.encode("utf-8")
                        batch.put(key, value)
                    batch.send()

                    hbase2 = happybase.Connection(**hbase_conf2)
                    period_table = f"data_{freq}_{user}_period"
                    try:
                        hbase2.create_table(period_table, {"info": {}, "v": {}})
                    except:
                        pass
                    hbase2 = happybase.Connection(**hbase_conf2)
                    ts_period = hbase2.table(period_table)
                    batch = ts_period.batch()
                    for key, value in data:
                        _, timestamp = key.decode().split("~")
                        key = f'{timestamp}~{new_d_id}'.encode("utf-8")
                        batch.put(key, value)
                    batch.send()


source = "gemweb"
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Mapping of Gemweb data to neo4j.')
    main_org_params = parser.add_argument_group("Organization",
                                                "Set the main organization information for importing the data")
    main_org_params.add_argument("--organization_name", "-name", help="The main organization name", required=True)
    main_org_params.add_argument("--user", "-u", help="The main organization name", required=True)
    main_org_params.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    main_org_params.add_argument("--type", "-t", help="The type to import [static] or [ts]", required=True)

    if os.getenv("PYCHARM_HOSTED"):
        args_t = ["-name", "Generalitat de Catalunya", "-n", "http://data.icaen.cat#", "-u", "icaen", "-t", "static"]
        args = parser.parse_args(args_t)
    else:
        args = parser.parse_args()
    # read config file
    with open("./config.json") as config_f:
        config = json.load(config_f)

    hbase_conn = config['hbase']
    hbase_conn2 = config['hbase_bigg']
    # get existing buildings num_ens
    neo4j_connection = {"uri": config['neo4j']['uri'],
                        "auth": (config['neo4j']['username'], config['neo4j']['password'])}
    n = Namespace(args.namespace)
    set_params(args.organization_name, source, n)
    if args.type == "static":
        static_mapping(args, source, hbase_conn, neo4j_connection)
    elif args.type == "ts":
        time_series_mapping(args, source, hbase_conn, hbase_conn2, neo4j_connection)

