from slugify import slugify

from rdf_utils.big_classes import Organization

ORGANIZATION_MAIN = None
source = None


def set_params(organization, s, namespace):
    global ORGANIZATION_MAIN
    ORGANIZATION_MAIN = organization
    global source
    source = s
    Organization.set_namespace(namespace)


def get_mappings(groups):
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
        }
    }

    org_lvl = {
        "name": f"organization_level",
        "class": Organization,
        "type": {
            "origin": "row",
        },
        "params": {
            "mapping": {
                "subject": {
                    "key": "name",
                    "operations": [slugify]
                },
                "organizationName": {
                    "key": "name",
                    "operations": []
                },
                "organizationDivisionType": {
                    "key": "type",
                    "operations": []
                }
            }
        }
    }
    org = {"main": [main_organization], "level": [org_lvl]}
    return org[groups]