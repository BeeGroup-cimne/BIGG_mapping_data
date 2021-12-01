from EEM.transform_functions import get_code_ens
from rdf_utils.bigg_definition import Bigg
from rdf_utils.big_classes import BuildingConstructionElement, EnergyEfficiencyMeasure
from slugify import slugify as slugify
from utils import *

ORGANIZATION_MAIN = None
source = None


def set_params(organization, s, namespace):
    global ORGANIZATION_MAIN
    ORGANIZATION_MAIN = organization
    global source
    source = s
    BuildingConstructionElement.set_namespace(namespace)
    EnergyEfficiencyMeasure.set_namespace(namespace)


def get_mappings(group):
    building_element = {
        "name": "building_element",
        "class": BuildingConstructionElement,
        "type": {
            "origin": "row"
        },
        "params": {
            "mapping": {
                "subject": {
                    "key": b'info:building_CodeEns_GPG',
                    "operations": [decode_hbase, get_code_ens, id_zfill, construction_element_subject]
                }
            }
        },
        "links": {
            "measures": {
                "type": Bigg.isAffectedByMeasures,
                "link": "id_"
            }
        }
    }

    measures = {
        "name": "measures",
        "class": EnergyEfficiencyMeasure,
        "type": {
            "origin": "row"
        },
        "params": {
            "raw": {
                "energyEfficiencyMeasureInvestmentCurrency": "€",
                "energyEfficiencyMeasureCurrencyExchangeRate": "1",
            },
            "mapping": {
                "subject": {
                    "key": [
                        {
                            "key": b'info:building_CodeEns_GPG',
                            "operations": [decode_hbase, get_code_ens, id_zfill]},
                        {
                            "key": "id_",
                            "operations": [decode_hbase]
                        },
                    ],
                    "operations": [partial(join_params, joiner="~"), eem_subject]
                },
                "energyEfficiencyMeasureType": {
                    "key": [
                        {
                            "key": b'info:improvement_type_level1',
                            "operations": [decode_hbase]},
                        {
                            "key": b'info:improvement_type_level2',
                            "operations": [decode_hbase]
                        },
                        {
                            "key": b'info:improvement_type_level3',
                            "operations": [decode_hbase]
                        },
                        {
                            "key": b'info:improvement_type_level4',
                            "operations": [decode_hbase]
                        },
                    ],
                    "operations": [partial(join_params, joiner=".")]
                },
                "energyEfficiencyMeasureDescription": {
                    "key": b'info:description',
                    "operations": [decode_hbase]
                },
                "shareOfAffectedElement": {
                    "key": b'info:improvement_percentage',
                    "operations": [decode_hbase]
                },
                "energyEfficiencyMeasureOperationalDate": {
                    "key": b'info:Data de finalitzaci\xc3\xb3 de l obra / millora',
                    "operations": [decode_hbase]
                },
                "energyEfficiencyMeasureInvestment": {
                    "key": b'info:investment_without_tax',
                    "operations": [decode_hbase]
                }

            }
        }
    }

    grouped_modules = {
        "all": [building_element, measures],
    }
    return grouped_modules[group]
