import json
import pandas as pd
import rdflib
from fuzzywuzzy import process
from neo4j import GraphDatabase
from rdflib import Namespace, Graph
from slugify import slugify
from GPG.GPG_mapping import set_params, get_mappings
from rdf_utils.bigg_definition import Bigg
from rdf_utils.rdf_functions import generate_rdf
from utils import save_rdf_with_source
source = "GPG"


def harmonize_organization_names(g, user_id, namespace, neo4j_conn):
    # Get all existing Organizations typed department
    neo = GraphDatabase.driver(**neo4j_conn)
    with neo.session() as s:
        organization_name = s.run(f"""MATCH (m:ns0__Organization {{ns0__userId: "{user_id}"}}) return m.ns0__organizationName""").single().value()
        organization_names = s.run(f"""
         MATCH (m:ns0__Organization {{ns0__userId: "{user_id}"}})-[*]->(n:ns0__Organization{{ns0__organizationDivisionType: "Department"}})
         RETURN n.uri
         """)
        dep_uri = [x.value() for x in organization_names]

    # Get all organizations in rdf graph using sparql
    query_department = f"""
        PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX bigg:<{str(Bigg)}>
        SELECT DISTINCT ?sub
        WHERE {{
            ?sub rdf:type bigg:Organization .
            ?sub bigg:organizationDivisionType "Department" .
        }}
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


def map_data(data, **kwargs):
    #organization_name = kwargs['organization_name']
    namespace = kwargs['namespace']
    user = kwargs['user']
    config = kwargs['config']
    organizations = kwargs['organizations'] if 'organizations' in kwargs else False

    n = Namespace(namespace)
    set_params(source, n)
    df = pd.DataFrame.from_records(data)
    if organizations:
        g = generate_rdf(get_mappings("all"), df)
        g = harmonize_organization_names(g, user, n, config['neo4j'])
    else:
        g = generate_rdf(get_mappings("buildings"), df)
    save_rdf_with_source(g, source, config['neo4j'])
