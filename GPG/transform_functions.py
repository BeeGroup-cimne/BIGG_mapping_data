import re
from functools import partial
import pandas as pd


def building_type_taxonomy(building_type):
    # Transformation function
    building_taxonomy_dict_temp = \
        pd.read_excel("GPG/taxonomy_buildingtype.xls", header=None, names=["A", "B"], index_col="A").to_dict()["B"]
    building_taxonomy_dict = {}
    for k, v in building_taxonomy_dict_temp.items():
        k_list = k.split(",")
        for k1 in k_list:
            k1 = k1.strip(" ")
            building_taxonomy_dict[k1] = [x.strip(" ") for x in v.split(",")]
    try:
        return building_taxonomy_dict[building_type]
    except KeyError:
        return ["Others"]


def clean_department(department):
    department = department.replace("--", "")
    department = department.replace("(Assignació)", "")
    department = department.replace("(Adscripció)", "")
    department = department.strip()
    return department


def find_type(department):
    if "(Assignació)" in department:
        return "Assignació"
    if "(Adscripció)" in department:
        return "Adscripció"
    return None


