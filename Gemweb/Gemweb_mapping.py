from Gemweb.transform_functions import ref_cadastral
from rdf_utils.bigg_definition import Bigg
from rdf_utils.big_classes import Organization, Building, LocationInfo, CadastralInfo, BuildingSpace, Area, Device, \
    BuildingConstructionElement, MeasurementList, BIGGObjects, UtilityPointOfDelivery
from slugify import slugify as slugify
from utils import *

ORGANIZATION_MAIN = None
source = None


def set_params(organization, s, namespace):
    global ORGANIZATION_MAIN
    ORGANIZATION_MAIN = organization
    global source
    source = s
    Organization.set_namespace(namespace)
    Building.set_namespace(namespace)
    LocationInfo.set_namespace(namespace)
    BuildingSpace.set_namespace(namespace)
    CadastralInfo.set_namespace(namespace)
    Area.set_namespace(namespace)
    BuildingConstructionElement.set_namespace(namespace)
    Device.set_namespace(namespace)
    MeasurementList.set_namespace(namespace)
    UtilityPointOfDelivery.set_namespace(namespace)


def get_mappings(group):

    main_organization = {
        "name": "main_organization",
        "class": Organization,
        "type": {
            "origin": "static",
        },
        "params": {
            "raw": {
                "subject": slugify(ORGANIZATION_MAIN),
                "organizationName": ORGANIZATION_MAIN,
                "organizationDivisionType": "Organization"
            }
        },
        "links": {
            "gemweb_link": {
                "type": Bigg.hasSource,
                "link": "__all__"
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
                    "key": "codi",
                    "operations": [decode_hbase, id_zfill, building_subject]
                },
                "buildingIDFromOrganization": {
                    "key": "codi",
                    "operations": [decode_hbase, id_zfill]
                },
                "buildingName": {
                    "key": "nom",
                    "operations": [decode_hbase, ]
                },
                "buildingUseType": {
                    "key": "subtipus",
                    "operations": [decode_hbase]#, building_type_taxonomy]
                },
                "buildingOwnership": {
                    "key": "responsable",
                    "operations": [decode_hbase]
                },
            }
        },
        "links": {
            "building_space": {
                "type": Bigg.hasSpace,
                "link": "dev_gem_id"
            },
            "location_info": {
                "type": Bigg.hasLocationInfo,
                "link": "dev_gem_id"
            },
            "cadastral_info": {
                "type": Bigg.hasCadastralInfos,
                "link": "dev_gem_id"
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
            "mapping": {
                "subject": {
                    "key": "codi",
                    "operations": [decode_hbase, id_zfill, location_info_subject]
                },
                "addressCountry": {
                    "key": "pais",
                    "operations": [decode_hbase]
                },
                "addressProvince": {
                    "key": "provincia",
                    "operations": [decode_hbase, ]
                },
                "addressCity": {
                    "key": "poblacio",
                    "operations": [decode_hbase, ]
                },
                "addressPostalCode": {
                    "key": "codi_postal",
                    "operations": [decode_hbase, ]
                },
                "addressStreetName": {
                    "key": "direccio",
                    "operations": [decode_hbase, ]
                },
                "addressLongitude": {
                    "key": "longitud",
                    "operations": [decode_hbase, ]
                },
                "addressLatitude": {
                    "key": "latitud",
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
            "operations": [decode_hbase, ref_cadastral, validate_ref_cadastral],
            "sep": ";",
            "column": "observacionsbuilding",
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
                    "key": "codi",
                    "operations": [decode_hbase, id_zfill, slugify, building_space_subject, ]
                },
                "buildingSpaceUseType": {
                    "key": "subtipus",
                    "operations": [decode_hbase]  # , building_type_taxonomy]
                }
            }
        },
        "links": {
            "gross_floor_area": {
                "type": Bigg.hasAreas,
                "link": "dev_gem_id"
            },
            "building_element": {
                "type": Bigg.isAssociatedWithElements,
                "link": "dev_gem_id"
            },
            "device": {
                "type": Bigg.isObservedBy,
                "link": "dev_gem_id"
            },
            "utility_point": {
                "type": Bigg.hasUtilityPointOfDelivery,
                "link": "dev_gem_id"
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
                    "key": "codi",
                    "operations": [decode_hbase, id_zfill, partial(gross_area_subject, a_source=source)]
                },
                "areaValue": {
                    "key": "superficie",
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
                    "key": "codi",
                    "operations": [decode_hbase, id_zfill, construction_element_subject]
                }
            }
        }
    }

    utility_point = {
        "name": "utility_point",
        "class": UtilityPointOfDelivery,
        "type": {
            "origin": "row"
        },
        "params": {
            "mapping": {
                "subject": {
                    "key": "cups",
                    "operations": [decode_hbase, delivery_subject]
                },
                "pointOfDeliveryIDFromUser": {
                    "key": "cups",
                    "operations": [decode_hbase, ]
                },
                "utilityType": {
                    "key": "tipus_submin",
                    "operations": [decode_hbase, ]
                }
            }
        },
        "links": {
            "device": {
                "type": Bigg.hasDevice,
                "link": "dev_gem_id"
            }
        }
    }
    device = {
        "name": "device",
        "class": Device,
        "type": {
            "origin": "row"
        },
        "params": {
            "mapping": {
                "subject": {
                    "key": "dev_gem_id",
                    "operations": [decode_hbase, partial(device_subject, source)]
                },
                "deviceName":  {
                    "key": "cups",
                    "operations": [decode_hbase, ]
                },
                "deviceType":  {
                    "key": "tipus_submin",
                    "operations": [decode_hbase, ]
                }
            }
        }
    }

    grouped_modules = {
        "linked": [main_organization, building, location_info, cadastral_info, building_space,
                   gross_floor_area, building_element, device, utility_point],
        "unlinked": [device]
    }
    return grouped_modules[group]
