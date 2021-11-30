from Gemweb.transform_functions import ref_cadastral
from rdf_utils.bigg_definition import Bigg
from rdf_utils.big_classes import Organization, Building, LocationInfo, CadastralInfo, BuildingSpace, Area, Device, \
    BuildingConstructionElement, MeasurementList, BIGGObjects
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
    Gemweb_source.set_namespace(namespace)


class Gemweb_source(BIGGObjects):
    __rdf_type__ = Bigg.GemwebSource

    def __init__(self, subject):
        super().__init__(subject)


def get_mappings(group):
    gemweb_link = {
        "name": "gemweb_link",
        "class": Gemweb_source,
        "type": {
            "origin": "static",
        },
        "params": {
            "raw": {
                "subject": slugify(f"Gemweb {ORGANIZATION_MAIN}"),
            }
        }
    }

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
                    "key": b'info:codi',
                    "operations": [decode_hbase, id_zfill, building_subject]
                },
                "buildingIDFromOrganization": {
                    "key": b'info:codi',
                    "operations": [decode_hbase, id_zfill]
                },
                "buildingName": {
                    "key": b'info:nom',
                    "operations": [decode_hbase, ]
                },
                "buildingUseType": {
                    "key": b'info:subtipus',
                    "operations": [decode_hbase]#, building_type_taxonomy]
                },
                "buildingOwnership": {
                    "key": b'info:responsable',
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
                    "key": b'info:codi',
                    "operations": [decode_hbase, id_zfill, location_info_subject]
                },
                "addressCountry": {
                    "key": b'info:pais',
                    "operations": [decode_hbase]
                },
                "addressProvince": {
                    "key": b'info:provincia',
                    "operations": [decode_hbase, ]
                },
                "addressCity": {
                    "key": b'info:poblacio',
                    "operations": [decode_hbase, ]
                },
                "addressPostalCode": {
                    "key": b'info:codi_postal',
                    "operations": [decode_hbase, ]
                },
                "addressStreetName": {
                    "key": b'info:direccio',
                    "operations": [decode_hbase, ]
                },
                "addressLongitude": {
                    "key": b'info:longitud',
                    "operations": [decode_hbase, ]
                },
                "addressLatitude": {
                    "key": b'info:latitud',
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
            "column": "b'info:observacions'building",
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
                    "key": b'info:codi',
                    "operations": [decode_hbase, id_zfill, slugify, building_space_subject, ]
                },
                "buildingSpaceUseType": {
                    "key": b'info:subtipus',
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
                    "key": b'info:codi',
                    "operations": [decode_hbase, id_zfill, partial(gross_area_subject, a_source=source)]
                },
                "areaValue": {
                    "key": b'info:superficie',
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
                    "key": b'info:codi',
                    "operations": [decode_hbase, id_zfill, construction_element_subject]
                }
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
                    "key": b'info:cups',
                    "operations": [decode_hbase, ]
                },
                "deviceType":  {
                    "key": b'info:tipus_submin',
                    "operations": [decode_hbase, ]
                }
            }
        },
        "links": {
            "gemweb_link": {
                "type": Bigg.importedFromSource,
                "link": "__all__"
            },
            "measures_list": {
                "type": Bigg.hasMeasurementLists,
                "link": "dev_gem_id"
            }
        }
    }

    grouped_modules = {
        "linked": [gemweb_link, main_organization, building, location_info, cadastral_info, building_space,
                   gross_floor_area, building_element, device],
        "unlinked": [gemweb_link, main_organization, device]
    }
    return grouped_modules[group]
