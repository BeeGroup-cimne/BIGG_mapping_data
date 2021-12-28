from copy import deepcopy
import pandas as pd
from rdflib import Graph

basic_mandatory_fields = ["name", "class", "type", "params"]
type_mandatory_fields = ["origin"]
static_params_mandatory_fields = ["raw"]
row_split_column_type_mandatory_fields = ["sep", "column", "column_mapping", "operations"]
row_split_column_params_mandatory_fields = ["column_mapping"]
row_params_mandatory_fields = ["mapping"]
mapping_params_mandatory_fields = ["key", "operations"]
links_mandatory_fields = ["type", "link"]


def __check_basic_syntax__(definition, mandatory_fields):
    for f in mandatory_fields:
        if f not in definition:
            raise SyntaxError(f"Error in definition: {f} is a mandatory field")


def generate_rdf(definitions, dataframe):
    g = Graph()
    for definition in definitions:
        __check_basic_syntax__(definition, basic_mandatory_fields)
    links = __organize_links__(definitions)
    for definition in definitions:
        name = definition['name']
        obj = definition['class']
        t = definition['type']
        params = definition['params']
        g += __distribute_origin__(obj, t, params, dataframe, name, links)
    links_g = __set_links__(links)
    g += links_g
    return g


def __organize_links__(definitions):
    links_origin = {}
    links_dest = {}
    links_fallback = {}
    link_dict = {"join": None, "predicate": None, "links": {}, "fallback": {}}
    for definition in definitions:
        links = definition['links'] if 'links' in definition else {}
        for k, v in links.items():
            __check_basic_syntax__(v, links_mandatory_fields)
            current_link = deepcopy(link_dict)
            current_link["join"] = v["link"]
            current_link["predicate"] = v["type"]
            try:
                links_origin[definition['name']].append(current_link)
            except:
                links_origin[definition['name']] = [current_link]
            try:
                links_dest[k].append(current_link)
            except:
                links_dest[k] = [current_link]
            if 'fallback' in v:
                current_link['fallback'] = {
                    "elements": [],
                    "bidirectional": v['fallback']['bidirectional'] if 'bidirectional' in v['fallback'] else None
                }
                try:
                    links_fallback[v['fallback']['key']].append(current_link)
                except:
                    links_fallback[v['fallback']['key']] = [current_link]

    return {"origin": links_origin, "dest": links_dest, "fallback": links_fallback}


def __distribute_origin__(obj, t, params, dataframe, name, links):
    __check_basic_syntax__(t, type_mandatory_fields)
    if t['origin'] == 'static':
        return __static_function__(obj, params, name, links)
    elif t['origin'] == 'row_split_column':
        return __row_split_column_function__(obj, t, params, dataframe, name, links)
    elif t['origin'] == 'row':
        return __row_function__(obj, params, dataframe, name, links)


def __raw_params__(params):
    return params['raw'] if 'raw' in params else {}


def __mapping_params__(params, row):
    mapping_dict = {}
    if 'mapping' not in params:
        return mapping_dict
    else:
        for k, v in params['mapping'].items():
            __check_basic_syntax__(v, mapping_params_mandatory_fields)
            if isinstance(v['key'], list):
                f_value = []
                for v1 in v['key']:
                    __check_basic_syntax__(v1, mapping_params_mandatory_fields)
                    if v1['key'] in row:
                        value = row[v1['key']]
                        if not value or value == b'nan' or pd.isna(value):
                            continue
                        for func in v1['operations']:
                            value = func(value)
                        f_value.append(value)
                    else:
                        continue
                for func in v['operations']:
                    f_value = func(f_value)
                mapping_dict[k] = f_value
            else:
                if v['key'] in row:
                    value = row[v['key']]
                    if value is None or value == b'nan':
                        continue
                    if isinstance(value, list):
                        if all(pd.isna(value)):
                            continue
                    else:
                        if pd.isna(value):
                            continue
                    for func in v['operations']:
                        value = func(value)
                    mapping_dict[k] = value
                else:
                    continue
    return mapping_dict


def __column_mapping_params__(params, column_mapping):
    mapping_dict = {}
    if 'column_mapping' not in params:
        return mapping_dict
    for k, v in params['column_mapping'].items():
        if v not in column_mapping:
            continue
        mapping_dict[k] = column_mapping[v]
    return mapping_dict


def __row_split_column_function__(obj, t, params, dataframe, name, links):
    __check_basic_syntax__(t, row_split_column_type_mandatory_fields)
    __check_basic_syntax__(params, row_split_column_params_mandatory_fields)
    g = Graph()
    for id_row, row in dataframe.iterrows():
        if t["column"] in row:
            column = row[t['column']]
            if not column or column == b'nan' or pd.isna(column):
                continue
            for func in t["operations"]:
                column = func(column)
            elements_list = column.split(t['sep'])
            for element in elements_list:
                if not element:
                    continue
                column_mapping = {}
                for k, v in t['column_mapping'].items():
                    value = element
                    for func in v:
                        value = func(value)
                    column_mapping[k] = value
                obj_params = {}
                obj_params.update(__raw_params__(params))
                obj_params.update(__mapping_params__(params, row))
                obj_params.update(__column_mapping_params__(params, column_mapping))
                g_i = __generate_object_rdf__(obj, obj_params)
                __manage_links__(g_i, links, name, row)
                g += g_i
    return g


def __row_function__(obj, params, dataframe, name, links):
    __check_basic_syntax__(params, row_params_mandatory_fields)
    g = Graph()
    for id_row, row in dataframe.iterrows():
        obj_params = {}
        obj_params.update(__raw_params__(params))
        obj_params.update(__mapping_params__(params, row))
        g_i = __generate_object_rdf__(obj, obj_params)
        __manage_links__(g_i, links, name, row)
        g += g_i
    return g


def __set_links__(links):
    g = Graph()
    links = links['origin']
    for _, link_struct in links.items():
        for link_type in link_struct:
            predicate = link_type['predicate']
            fallback = link_type['fallback']
            for _, linked_elements in link_type['links'].items():
                subject_set = linked_elements['subject']
                object_set = linked_elements['object']
                for s in subject_set:
                    if object_set:
                        for o in object_set:
                            g.add((s, predicate, o))
                    else:
                        if fallback:
                            bidirection = fallback['bidirectional'] if 'bidirectional' in fallback else None
                            for o in fallback['elements']:
                                g.add((s, predicate, o))
                                if bidirection:
                                    g.add((o, bidirection, s))

    return g


def __manage_links__(graph, links, name, row):
    subject = list(set(graph.subjects()))[0]
    if name in links['origin']:
        for link in links['origin'][name]:
            if link['join'] == "__all__":
                if '__all__' not in link['links']:
                    link['links']["__all__"] = {"subject": set(), "object": set()}
                link['links']["__all__"]["subject"].add(subject)
            else:
                if row[link['join']] not in link['links']:
                    link['links'][row[link['join']]] = {"subject": set(), "object": set()}
                link['links'][row[link['join']]]["subject"].add(subject)

    if name in links['dest']:
        for link in links['dest'][name]:
            if link['join'] == "__all__":
                if '__all__' not in link['links']:
                    link['links']["__all__"] = {"subject": set(), "object": set()}
                link['links']["__all__"]["object"].add(subject)

            else:
                if row[link['join']] not in link['links']:
                    link['links'][row[link['join']]] = {"subject": set(), "object": set()}
                link['links'][row[link['join']]]["object"].add(subject)

    if name in links['fallback']:
        for link in links['fallback'][name]:
            link['fallback']['elements'].append(subject)


def __static_function__(obj, params, name, links):
    __check_basic_syntax__(params, static_params_mandatory_fields)
    obj_params = {}
    obj_params.update(__raw_params__(params))
    graph = __generate_object_rdf__(obj, obj_params)
    __manage_links__(graph, links, name, None)
    return graph


def __generate_object_rdf__(obj, params):
    ele = obj(**params)
    return ele.get_graph()

