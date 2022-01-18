import json
from urllib.parse import urlparse

import pandas as pd
from neo4j import GraphDatabase
from rdflib import Namespace
from slugify import slugify
from Gemweb.Gemweb_mapping import set_params, get_mappings
from rdf_utils.rdf_functions import generate_rdf
from utils import decode_hbase, save_rdf_with_source, link_devices_with_source

source = "gemweb"


def map_data(data, **kwargs):
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']

    neo = GraphDatabase.driver(**config['neo4j'])
    n = Namespace(namespace)
    set_params(source, n)
    with neo.session() as ses:
        source_id = ses.run(
            f"""Match (o: ns0__Organization{{ns0__userId: "{user}"}})-[:ns0__hasSource]->(s:GemwebSource) 
                return id(s)""")
        source_id = source_id.single().get("id(s)")

    ids_ens = []
    with neo.session() as ses:
        buildings_neo = ses.run(
            f"""Match (n:ns0__Building)<-[*]-(o:ns0__Organization)-[:ns0__hasSource]->(s:GemwebSource) 
                Where id(s)={source_id} 
                return n.uri""")
        ids_ens = list(set([urlparse(x.get("n.uri")).fragment.split("-")[1] for x in buildings_neo]))
    # create num_ens column with parsed values in df
    df = pd.DataFrame.from_records(data)
    df['num_ens'] = df['codi'].apply(lambda x: decode_hbase(x).zfill(5))

    # get all devices with linked buildings
    df_linked = df[df['num_ens'].isin([str(i) for i in ids_ens])]

    g = generate_rdf(get_mappings("linked"), df_linked)
    save_rdf_with_source(g, source, config['neo4j'])

    # link devices from G to the source
    link_devices_with_source(g, source_id, config['neo4j'])

    df_unlinked = df[df['num_ens'].isin([str(i) for i in ids_ens]) == False]
    g2 = generate_rdf(get_mappings("unlinked"), df_unlinked)
    save_rdf_with_source(g2, source, config['neo4j'])
    link_devices_with_source(g2, source_id, config['neo4j'])