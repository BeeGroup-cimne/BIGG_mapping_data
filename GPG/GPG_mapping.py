from rdf_utils.bigg_definition import Bigg
from rdf_utils.big_classes import Organization, Building, LocationInfo, CadastralInfo, BuildingSpace, Area, \
    BuildingConstructionElement
from slugify import slugify as slugify
from GPG.transform_functions import *
from utils import *

source = None


def set_params(s, namespace):
    global source
    source = s
    Organization.set_namespace(namespace)
    Building.set_namespace(namespace)
    LocationInfo.set_namespace(namespace)
    BuildingSpace.set_namespace(namespace)
    CadastralInfo.set_namespace(namespace)
    Area.set_namespace(namespace)
    BuildingConstructionElement.set_namespace(namespace)


def get_mappings(group):
    others_organization = {
        "name": "others_organization",
        "class": Organization,
        "type": {
            "origin": "static",
        },
        "params": {
            "raw": {
                "subject": slugify("altres"),
                "organizationName": "Altres",
                "organizationDivisionType": "Department"
            }
        }
    }

    department_organization = {
        "name": "department_organization",
        "class": Organization,
        "type": {
            "origin": "row_split_column",
            "operations": [decode_hbase, ],
            "sep": ";",
            "column": "Departament_Assig_Adscrip",
            "column_mapping": {
                "subject": [clean_department, slugify],
            }
        },
        "params": {
            "raw": {
                "organizationDivisionType": "Department"
            },
            "column_mapping": {
                "subject": "subject",
            }
        },
        "links": {
            # "main_organization": {
            #     "type": Bigg.hasSuperOrganization,
            #     "link": "__all__"
            # },
            "building_organization": {
                "type": Bigg.hasSubOrganization,
                "link": "Num_Ens_Inventari"
            }
        }
    }

    building_organization = {
        "name": "building_organization",
        "class": Organization,
        "type": {
            "origin": "row"
        },
        "params": {
            "raw": {
                "organizationDivisionType": "Building"
            },
            "mapping": {
                "subject": {
                    "key": "Num_Ens_Inventari",
                    "operations": [decode_hbase, id_zfill, building_department_subject]
                },
                "organizationName": {
                    "key": "Espai",
                    "operations": [decode_hbase, ]
                }
            }
        },
        "links": {
            # "department_organization": {
            #     "type": Bigg.hasSuperOrganization,
            #     "link": "Num_Ens_Inventari",
            #     "fallback": {
            #         "key": "main_organization",
            #         "bidirectional": Bigg.hasSubOrganization
            #     }
            # },
            "building": {
                "type": Bigg.managesBuilding,
                "link": "Num_Ens_Inventari"
            }
        }
    }

    building = {
        "name": "building",
        "class": Building,
        "type": {
            "origin": "row"
        },
        "params": {
            "mapping": {
                "subject": {
                    "key": "Num_Ens_Inventari",
                    "operations": [decode_hbase, id_zfill,  building_subject]
                },
                "buildingIDFromOrganization": {
                    "key": "Num_Ens_Inventari",
                    "operations": [decode_hbase, id_zfill]
                },
                "buildingName": {
                    "key": "Espai",
                    "operations": [decode_hbase, ]
                },
                "buildingUseType": {
                    "key": "Tipus_us",
                    "operations": [decode_hbase, building_type_taxonomy]
                }
            }
        },
        "links": {
            # "building_organization": {
            #     "type": Bigg.pertainsToOrganization,
            #     "link": "Num_Ens_Inventari"
            # },
            "building_space": {
                "type": Bigg.hasSpace,
                "link": "Num_Ens_Inventari"
            },
            "location_info": {
                "type": Bigg.hasLocationInfo,
                "link": "Num_Ens_Inventari"
            },
            "cadastral_info": {
                "type": Bigg.hasCadastralInfos,
                "link": "Num_Ens_Inventari"
            }
        }
    }

    location_info = {
        "name": "location_info",
        "class": LocationInfo,
        "type": {
            "origin": "row"
        },
        "params": {
            "raw": {
                "addressCountry": "Catalonia"
            },
            "mapping": {
                "subject": {
                    "key": "Num_Ens_Inventari",
                    "operations": [decode_hbase, id_zfill,  location_info_subject]
                },
                "addressProvince": {
                    "key": "Provincia",
                    "operations": [decode_hbase, ]
                },
                "addressCity": {
                    "key": "Municipi",
                    "operations": [decode_hbase, ]
                },
                "addressPostalCode": {
                    "key": "Codi_postal",
                    "operations": [decode_hbase, ]
                },
                "addressStreetNumber": {
                    "key": "Num_via",
                    "operations": [decode_hbase, ]
                },
                "addressStreetName": {
                    "key": "Via",
                    "operations": [decode_hbase, ]
                }
            }
        }
    }


    cadastral_info = {
        "name": "cadastral_info",
        "class": CadastralInfo,
        "type": {
            "origin": "row_split_column",
            "operations": [decode_hbase, validate_ref_cadastral],
            "sep": ";",
            "column": "Ref_Cadastral",
            "column_mapping": {
                "subject": [str.strip],
                "landCadastralReference": [str.strip]
            }
        },
        "params": {
            "column_mapping": {
                "subject": "subject",
                "landCadastralReference": "landCadastralReference"
            },
            "mapping": {
                "landArea": {
                    "key": "Sup_terreny",
                    "operations": [decode_hbase, ]
                },
                "landType": {
                    "key": "Classificacio_sol",
                    "operations": [decode_hbase, ]
                }
            }
        }
    }

    building_space = {
        "name": "building_space",
        "class": BuildingSpace,
        "type": {
            "origin": "row"
        },
        "params": {
            "raw": {
                "buildingSpaceName": "Building",
            },
            "mapping": {
                "subject": {
                    "key": "Num_Ens_Inventari",
                    "operations": [decode_hbase, id_zfill,  building_space_subject]
                },
                "buildingSpaceUseType": {
                    "key": "Tipus_us",
                    "operations": [decode_hbase, building_type_taxonomy]
                }
            }
        },
        "links": {
            "gross_floor_area": {
                "type": Bigg.hasAreas,
                "link": "Num_Ens_Inventari"
            },
            "gross_floor_area_above_ground": {
                "type": Bigg.hasAreas,
                "link": "Num_Ens_Inventari"
            },
            "gross_floor_area_under_ground": {
                "type": Bigg.hasAreas,
                "link": "Num_Ens_Inventari"
            },
            "building_element": {
                "type": Bigg.isAssociatedWithElements,
                "link": "Num_Ens_Inventari"
            }
        }
    }

    gross_floor_area = {
        "name": "gross_floor_area",
        "class": Area,
        "type": {
            "origin": "row"
        },
        "params": {
            "raw": {
                "areaType": "GrossFloorArea",
                "areaUnitOfMeasurement": "m2",
            },
            "mapping": {
                "subject": {
                    "key": "Num_Ens_Inventari",
                    "operations": [decode_hbase,  id_zfill, partial(gross_area_subject, a_source=source)]
                },
                "areaValue": {
                    "key": "Sup_const_total",
                    "operations": [decode_hbase, ]
                }
            }
        }
    }

    gross_floor_area_above_ground = {
        "name": "gross_floor_area_above_ground",
        "class": Area,
        "type": {
            "origin": "row"
        },
        "params": {
            "raw": {
                "areaType": "GrossFloorAreaAboveGround",
                "areaUnitOfMeasurement": "m2",
            },
            "mapping": {
                "subject": {
                    "key": "Num_Ens_Inventari",
                    "operations": [decode_hbase, id_zfill, partial(gross_area_subject_above, a_source=source)]
                },
                "areaValue": {
                    "key": "Sup_const_sobre_rasant",
                    "operations": [decode_hbase, ]
                }
            }
        }
    }

    gross_floor_area_under_ground = {
        "name": "gross_floor_area_under_ground",
        "class": Area,
        "type": {
            "origin": "row"
        },
        "params": {
            "raw": {
                "areaType": "GrossFloorAreaUnderGround",
                "areaUnitOfMeasurement": "m2",
            },
            "mapping": {
                "subject": {
                    "key": "Num_Ens_Inventari",
                    "operations": [decode_hbase, id_zfill, partial(gross_area_subject_under, a_source=source)]
                },
                "areaValue": {
                    "key": "Sup_const_sota rasant",
                    "operations": [decode_hbase, ]
                }
            }
        }
    }
    building_element = {
        "name": "building_element",
        "class": BuildingConstructionElement,
        "type": {
            "origin": "row"
        },
        "params": {
            "raw": {
                "buildingConstructionElementType": "Building",
            },
            "mapping": {
                "subject": {
                    "key": "Num_Ens_Inventari",
                    "operations": [decode_hbase, id_zfill, construction_element_subject]
                }
            }
        }
    }
    grouped_modules = {
        "all": [department_organization, building_organization, building, location_info, cadastral_info, building_space,
                gross_floor_area, gross_floor_area_under_ground, gross_floor_area_above_ground, building_element],
        "buildings": [building_organization, building, location_info, cadastral_info, building_space,
                      gross_floor_area, gross_floor_area_under_ground, gross_floor_area_above_ground, building_element],
        "other": [others_organization]
    }
    return grouped_modules[group]
