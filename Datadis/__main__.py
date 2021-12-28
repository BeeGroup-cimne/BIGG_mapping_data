import argparse
import hashlib
import json
import re
import urllib

import happybase
import pandas as pd
from rdflib import Namespace
from Datadis.Datadis_mapping import *
from rdf_utils.rdf_functions import generate_rdf
from utils import save_rdf_with_source


def static_mapping(args, source, hbase_conn, neo4j_connection):
    # get supplies from HBASE
    supplies_table = f"{source}_supplies_{args.user}".lower()
    neo = GraphDatabase.driver(**neo4j_connection)
    with neo.session() as ses:
        datadis_source = ses.run(f"""
            Match (u:ns0__UtilityPointOfDelivery)<-[*]-(b:ns0__Building)<-[*]-(o:ns0__Organization{{user_id:"{args.user}"}})
            return u.ns0__pointOfDeliveryIDFromUser , b.ns0__buildingIDFromOrganization
            """
                                 )
        cups_bcode = {x['u.ns0__pointOfDeliveryIDFromUser']: x['b.ns0__buildingIDFromOrganization'] for x in datadis_source}

    for data in get_hbase_data_batch(hbase_conn, supplies_table, batch_size=100000):
        print("starting")
        supplies = []
        for cups, data1 in data:
            data1.update({"cups": cups})
            supplies.append(data1)
        supplies_df = pd.DataFrame.from_records(supplies)
        if supplies_df.empty:
            continue
        supplies_df['decoded_cups'] = supplies_df.cups.apply(bytes.decode)
        supplies_df['NumEns'] = supplies_df.decoded_cups.apply(lambda x: cups_bcode[x] if x in cups_bcode else None)
        linked_supplies = supplies_df[supplies_df["NumEns"].isna()==False]
        unlinked_supplies = supplies_df[supplies_df["NumEns"].isna()]
        for linked, df in [("linked", linked_supplies), ("unlinked", unlinked_supplies)]:
            for group, supply_by_group in df.groupby(b"info:nif"):
                print(f"generating_rdf for {group}, {linked},{len(supply_by_group)}")
                if supply_by_group.empty:
                    continue
                with neo.session() as ses:
                    datadis_source = ses.run(
                        f"""Match (n: DatadisSource{{username:"{group.decode()}"}}) return n""").single()
                    datadis_source = datadis_source.get("n").id
                print("generating rdf")
                g = generate_rdf(get_mappings(linked), supply_by_group)
                print("saving to neo4j")
                save_rdf_with_source(g, source, neo4j_connection)
                print("linking with source")
                link_devices_with_source(g, datadis_source, neo4j_connection)


def time_series_mapping(args, source, hbase_conf, hbase_conf2, neo4j_connection):
    neo = GraphDatabase.driver(**neo4j_connection)

    with neo.session() as ses:
        source_id = ses.run(
            f"""Match (o: ns0__Organization{{uri:'{args.namespace}{slugify(args.organization_name)}'}})-[:ns0__hasSource]->(s:GemwebSource) 
            return id(s)""")
        source_id = source_id.single().get("id(s)")
    with neo.session() as session:
        devices_neo = session.run(f"""
        MATCH (g)<-[:ns0__importedFromSource]-(d) WHERE id(g)={source_id}
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
                prefix = (device_id + '~').encode("utf-8")
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


source = "datadis"
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Mapping of Datadis data to neo4j.')
    main_org_params = parser.add_argument_group("Organization",
                                                "Set the main organization information for importing the data")
    main_org_params.add_argument("--organization_name", "-name", help="The main organization name", required=True)
    main_org_params.add_argument("--user", "-u", help="The main organization name", required=True)
    main_org_params.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    main_org_params.add_argument("--type", "-t", help="The type to import [static] or [ts]", required=True)

    if os.getenv("PYCHARM_HOSTED"):
        args_t = ["-name", "Generalitat de Catalunya", "-n", "http://icaen.cat#", "-u", "icaen", "-t", "static"]
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

