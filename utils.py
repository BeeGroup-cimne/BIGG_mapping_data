import re
from functools import partial
from urllib.parse import urlparse
import happybase
import rdflib
from neo4j import GraphDatabase
from rdflib import Graph, RDF
from rdf_utils.bigg_definition import Bigg
import base64
import hashlib
from Crypto.Cipher import AES
from Crypto import Random
import os


multi_value_classes = [Bigg.LocationInfo, Bigg.CadastralInfo, Bigg.Building, Bigg.BuildingSpace]


def decode_hbase(value):
    if value is not None:
        return value.decode()
    return ""


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
