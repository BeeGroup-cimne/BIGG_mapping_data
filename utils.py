import json
import pickle
import re
import time
import uuid
from datetime import datetime
from functools import partial
from urllib.parse import urlparse
import happybase
import rdflib
from kafka import KafkaConsumer, KafkaProducer
from neo4j import GraphDatabase
from pymongo import MongoClient
from rdflib import Graph, RDF
from rdf_utils.bigg_definition import Bigg
import base64
import hashlib
from Crypto.Cipher import AES
from Crypto import Random
import os

multi_value_classes = [Bigg.LocationInfo, Bigg.CadastralInfo, Bigg.Building, Bigg.BuildingSpace]


def decode_hbase(value):
    if value is None:
        return ""
    elif isinstance(value, bytes):
        return value.decode()
    else:
        return str(value)


def join_params(args, joiner='~'):
    return joiner.join(args)


def zfill_param(key, num):
    return key.zfill(num)


id_zfill = partial(zfill_param, num=5)


def building_department_subject(key):
    return f"ORGANIZATION-{key}"


def building_subject(key):
    return f"BUILDING-{key}"


def building_space_subject(key):
    return f"BUILDINGSPACE-{key}"


def location_info_subject(key):
    return f"LOCATION-{key}"


def __area_subject__(key, a_type, a_source):
    return f"AREA-{a_type}-{a_source}-{key}"


gross_area_subject = partial(__area_subject__, a_type="GrossFloorArea")
gross_area_subject_above = partial(__area_subject__, a_type="GrossFloorAreaAboveGround")
gross_area_subject_under = partial(__area_subject__, a_type="GrossFloorAreaUnderGround")


def construction_element_subject(key):
    return f"ELEMENT-{key}"


def eem_subject(key):
    return f"EEM-{key}"


def device_subject(key, source):
    return f"{source}-DEVICE-{key}"

def delivery_subject(key):
    return f"SUPPLY-{key}"

def device_raw_measure_subject(key, source):
    return f"{source}-DEVICE-RAW-{key}"


def validate_ref_cadastral(value):
    ref = value.split(";")
    valid_ref = []
    for refer in ref:
        refer = refer.strip()
        match = re.match("[0-9A-Z]{20}", refer)
        if match:
            valid_ref.append(match[0])
    return ";".join(valid_ref)


def read_config(conf_file):
    with open(conf_file) as config_f:
        config = json.load(config_f)
        config['neo4j']['auth'] = tuple(config['neo4j']['auth'])
        return config


def __neo4j_import__(ses, v):
    f = f"""CALL n10s.rdf.import.inline('{v}','Turtle')"""
    result = ses.run(f)
    return result.single()


def save_rdf_with_source(graph, source, connection):
    neo = GraphDatabase.driver(**connection)
    # only multi_value_classes will have "multiple_values"
    multi_value_subjects = {}
    for class_ in multi_value_classes:
        multi_value_subjects[class_] = list(set(graph.subjects(RDF.type, class_)))
    g2 = Graph()
    for class_, list_ in multi_value_subjects.items():
        if not list_:
            continue
        # get and parse the elements existing in DB
        parsed_type = urlparse(class_)
        with neo.session() as session:
            neo_data = session.run(f"Match (n: ns0__{parsed_type.fragment}) return n")
            if neo_data:
                neo_elements = {neo_element['n'].get("uri"): neo_element for neo_element in neo_data}
            else:
                neo_elements = {}
        for subject in list_:
            try:
                neo_element = neo_elements[str(subject)]
            except:
                neo_element = None
            for s, p, o in graph.triples((subject, None, None)):
                if isinstance(o, rdflib.Literal):
                    parsed_uri = urlparse(p)
                    if not neo_element or not neo_element['n'].get(f'ns0__{parsed_uri.fragment}'):
                        g2.add((s, p, o))
                        g2.add((s, p + '__selected', rdflib.Literal(source)))
                    g2.add((s, p + f"__{source}", o))
                else:
                    g2.add((s, p, o))
            graph.remove((subject, None, None))

    g2 += graph
    v = g2.serialize(format="ttl")
    v = v.replace('\\"', "`")
    v = v.replace("'", "`")

    with neo.session() as session:
        tty = __neo4j_import__(session, v)
        print(tty)


def link_devices_with_source(g, source_id, neo4j_connection):
    query_devices = f"""
               PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
               PREFIX bigg:<{str(Bigg)}>
               SELECT DISTINCT ?sub
               WHERE {{
                   ?sub rdf:type bigg:Device .
               }}    
            """
    r_devices = g.query(query_devices)
    neo = GraphDatabase.driver(**neo4j_connection)
    with neo.session() as session:
        for subject in r_devices:
            session.run(
                f"""
                    MATCH (source) WHERE id(source)={source_id}
                    MATCH (device) WHERE device.uri="{str(subject[0])}"
                    Merge (source)<-[:ns0__importedFromSource]-(device)
                    RETURN device""")


def __get_h_table__(hbase, table_name, cf=None):
    try:
        if not cf:
            cf = {"cf": {}}
        hbase.create_table(table_name, cf)
    except Exception as e:
        if str(e.__class__) == "<class 'Hbase_thrift.AlreadyExists'>":
            pass
        else:
            print(e)
    return hbase.table(table_name)


def save_to_hbase(documents, h_table_name, hbase_connection, cf_mapping, row_fields=None, batch_size=1000):
    hbase = happybase.Connection(**hbase_connection)
    table = __get_h_table__(hbase, h_table_name, {cf: {} for cf, _ in cf_mapping})
    h_batch = table.batch(batch_size=batch_size)
    row_auto = 0
    uid = uuid.uuid4()
    for d in documents:
        if not row_fields:
            row = f"{uid}~{row_auto}"
            row_auto += 1
        else:
            row = "~".join([str(d.pop(f)) if f in d else "" for f in row_fields])
        values = {}
        for cf, fields in cf_mapping:
            if fields == "all":
                for c, v in d.items():
                    values["{cf}:{c}".format(cf=cf, c=c)] = str(v)
            else:
                for c in fields:
                    if c in d:
                        values["{cf}:{c}".format(cf=cf, c=c)] = str(d[c])
        h_batch.put(str(row), values)
    h_batch.send()


def get_hbase_data_batch(hbase_conf, hbase_table, row_start=None, row_stop=None, row_prefix=None, columns=None,
                         _filter=None, timestamp=None, include_timestamp=False, batch_size=100000,
                         scan_batching=None, limit=None, sorted_columns=False, reverse=False):

    if row_prefix:
        row_start = row_prefix
        row_stop = row_prefix[:-1]+chr(row_prefix[-1]+1).encode("utf-8")

    if limit:
        if limit > batch_size:
            current_limit = batch_size
        else:
            current_limit = limit
    else:
        current_limit = batch_size
    current_register = 0
    while True:
        hbase = happybase.Connection(**hbase_conf)
        table = hbase.table(hbase_table)
        data = list(table.scan(row_start=row_start, row_stop=row_stop, columns=columns, filter=_filter,
                               timestamp=timestamp, include_timestamp=include_timestamp, batch_size=batch_size,
                               scan_batching=scan_batching, limit=current_limit, sorted_columns=sorted_columns,
                               reverse=reverse))
        if not data:
            break
        last_record = data[-1][0]
        current_register += len(data)
        yield data

        if limit:
            if current_register >= limit:
                break
            else:
                current_limit = min(batch_size, limit - current_register)
        row_start = last_record[:-1] + chr(last_record[-1] + 1).encode("utf-8")
    yield []


def pad(s):
    """
    pad with spaces at the end of the text beacuse AES needs 16 byte blocks
    :param s:
    :return:
    """
    block_size = 16
    remainder = len(s) % block_size
    padding_needed = block_size - remainder
    return s + padding_needed * ' '


def un_pad(s):
    """
    remove the extra spaces at the end
    :param s:
    :return:
    """
    return s.rstrip()


def encrypt(plain_text, password):
    # generate a random salt
    salt = os.urandom(AES.block_size)

    # generate a random iv
    iv = Random.new().read(AES.block_size)

    # use the Scrypt KDF to get a private key from the password
    private_key = hashlib.scrypt(password.encode(), salt=salt, n=2 ** 14, r=8, p=1, dklen=32)

    # pad text with spaces to be valid for AES CBC mode
    padded_text = pad(plain_text)

    # create cipher config
    cipher_config = AES.new(private_key, AES.MODE_CBC, iv)

    # return a dictionary with the encrypted text
    return {
        'cipher_text': base64.b64encode(cipher_config.encrypt(padded_text)),
        'salt': base64.b64encode(salt),
        'iv': base64.b64encode(iv)
    }


def decrypt(enc_dict, password):
    # decode the dictionary entries from base64
    salt = base64.b64decode(enc_dict['salt'])
    enc = base64.b64decode(enc_dict['cipher_text'])
    iv = base64.b64decode(enc_dict['iv'])

    # generate the private key from the password and salt
    private_key = hashlib.scrypt(password.encode(), salt=salt, n=2 ** 14, r=8, p=1, dklen=32)

    # create the cipher config
    cipher = AES.new(private_key, AES.MODE_CBC, iv)

    # decrypt the cipher text
    decrypted = cipher.decrypt(enc)

    # unpad the text to remove the added spaces
    original = un_pad(decrypted)

    return original


def read_from_kafka(topic, group_id, config):
    kafka_servers = [f"{host}:{port}" for host, port in zip(config['hosts'], config['ports'])]
    consumer = KafkaConsumer(topic, bootstrap_servers=kafka_servers, group_id=group_id,
                             value_deserializer=lambda v: pickle.loads(v))
    for m in consumer:
        yield m


class mongo_logger(object):
    mongo_conf = None
    collection = None

    log_id = None
    db = None
    log_type = None

    @staticmethod
    def __connect__(mongo_conf, collection):
        mongo_logger.mongo_conf = mongo_conf
        mongo_logger.collection = collection
        mongo = mongo_logger.connection_mongo(mongo_logger.mongo_conf)
        mongo_logger.db = mongo[mongo_logger.collection]

    @staticmethod
    def create(mongo_conf, collection, log_type,  user):
        mongo_logger.__connect__(mongo_conf, collection)
        mongo_logger.log_type = log_type
        log_document = {
            "user": user,
            "logs": {
                "gather": [],
                "store": [],
                "harmonize": []
            }
        }
        mongo_logger.log_id = mongo_logger.db.insert_one(log_document).inserted_id

    @staticmethod
    def export_log():
        return {
            "mongo_conf": mongo_logger.mongo_conf,
            "collection": mongo_logger.collection,
            "log_id": mongo_logger.log_id
        }

    @staticmethod
    def import_log(exported_info, log_type):
        mongo_logger.__connect__(exported_info['mongo_conf'], exported_info['collection'])
        mongo_logger.log_id = exported_info['log_id']
        mongo_logger.log_type = log_type

    @staticmethod
    def log(message):
        if any([mongo_logger.db is None, mongo_logger.db is None, mongo_logger.log_type is None]):
            return
        mongo_logger.db.update_one({"_id": mongo_logger.log_id},
                                   {"$push": {
                                       f"logs.{mongo_logger.log_type}": f"{datetime.utcnow()}: \
                                       {message}"}})

    # MongoDB functions
    @staticmethod
    def connection_mongo(config):
        cli = MongoClient("mongodb://{user}:{pwd}@{host}:{port}/{db}".format(**config))
        db = cli[config['db']]
        return db

# def read_from_kafka(topic, config):
#     kafka_servers = [f"{host}:{port}" for host, port in zip(config['hosts'], config['ports'])]
#     consumer = KafkaConsumer(topic, bootstrap_servers=kafka_servers,
#                              key_deserializer=lambda v: v.decode('utf-8'),
#                              value_deserializer=lambda v: json.loads(v.decode("utf-8")))
#     producer = KafkaProducer(bootstrap_servers=kafka_servers,
#                              value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#                              key_serializer=lambda v: v.encode('utf-8'),
#                              compression_type='gzip')
#
#     for m in consumer:
#         data_read = []
#         message_id = m.key
#         print(f"received  message_id: {message_id}")
#         while True:
#             meta_message = m.value
#             if meta_message['message_type'] == "meta":
#                 message = meta_message
#                 message['data'] = []
#                 data_read.append(message)
#                 tmp_mess = None
#                 for m in consumer:
#                     if m.key != message_id:
#                         # if it is from another message_id, add it to the end of the kafka topic
#                         f = producer.send(topic, key=m.key, value=m.value)
#                         continue
#                     tmp_mess = m.value
#                     if tmp_mess['message_type'] == "data":
#                         message['data'].extend(tmp_mess['data'])
#                     else:
#                         break
#                 if tmp_mess and tmp_mess['message_type'] == "end":
#                     yield data_read
#                     break
#             else:
#                 print("message without meta")
#                 break
