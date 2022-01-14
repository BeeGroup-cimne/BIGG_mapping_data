import pandas as pd
from neo4j import GraphDatabase
from rdflib import Namespace

from EEM.EEM_mapping import get_mappings, set_params
from EEM.transform_functions import get_code_ens
from rdf_utils.rdf_functions import generate_rdf
from utils import decode_hbase, id_zfill, save_rdf_with_source


def map_data(data, **kwargs):
    config = kwargs['config']
    source = kwargs['source']
    user = kwargs['user']
    namespace = kwargs['namespace']
    organization_name = kwargs['organization_name']
    df = pd.DataFrame.from_records(data)
    if df.empty:
        return
    df['ce'] = df['building_CodeEns_GPG'].apply(decode_hbase)
    df['ce'] = df['ce'].apply(get_code_ens)
    df['ce'] = df['ce'].apply(id_zfill)
    # upload only mapped measures
    df = df[df['ce'] != '00000']
    # Get all existing BuildingConstructionElements
    neo = GraphDatabase.driver(**config['neo4j'])
    with neo.session() as s:
        element_id = s.run(f"""
                            MATCH (o: ns0__Organization{{ns0__userId: "{user}"}})-[*]->(n:ns0__BuildingConstructionElement {{ns0__buildingConstructionElementType: 'Building'}})
                            RETURN n.uri
                            """)
        uri = [get_code_ens(x.value()) for x in element_id]
    df = df[df["ce"].isin(uri)]
    n = Namespace(namespace)
    set_params(organization_name, source, n)
    g = generate_rdf(get_mappings("all"), df)
    save_rdf_with_source(g, source, config['neo4j'])


