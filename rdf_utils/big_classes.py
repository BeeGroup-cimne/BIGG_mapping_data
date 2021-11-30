from rdflib import RDF, Literal, Graph
from rdf_utils.bigg_definition import Bigg


class BIGGObjects(object):
    __rdf_type__ = "__invalid__"
    __rdf_namespace__ = "__invalid__"

    @classmethod
    def set_namespace(cls, namespace):
        cls.__rdf_namespace__ = namespace

    def __init__(self, subject):
        self.subject = self.__rdf_namespace__[subject]
        if self.__class__ == BIGGObjects.__class__:
            raise NotImplementedError("The BiggObject is abstract")

    def get_graph(self):
        g = Graph()
        g.add((self.subject, RDF.type, self.__rdf_type__))
        for k, v in vars(self).items():
            if k != "subject" and v:
                g.add((self.subject, Bigg[k], Literal(v)))
        return g


class Organization(BIGGObjects):
    __rdf_type__ = Bigg.Organization

    def __init__(self, subject, organizationName=None, organizationType=None, organizationDivisionType=None,
                 organizationContactPersonName=None, organizationEmail=None, organizationTelephoneNumber=None):
        super().__init__(subject)
        self.organizationName = organizationName
        self.organizationType = organizationType
        self.organizationDivisionType = organizationDivisionType
        self.organizationContactPersonName = organizationContactPersonName
        self.organizationEmail = organizationEmail
        self.organizationTelephoneNumber = organizationTelephoneNumber


class Building(BIGGObjects):
    __rdf_type__ = Bigg.Building

    def __init__(self, subject, buildingIDFromOrganization=None,  buildingName=None, buildingConstructionYear=None,
                 buildingConstructionElementType=None, buildingUseType=None, buildingOwnership=None,
                 buildingOpeningHour=None, buildingClosingHour=None):
        super().__init__(subject)
        self.buildingIDFromOrganization = buildingIDFromOrganization
        self.buildingName = buildingName
        self.buildingConstructionYear = buildingConstructionYear
        self.buildingConstructionElementType = buildingConstructionElementType
        self.buildingUseType = buildingUseType
        self.buildingOwnership = buildingOwnership
        self.buildingOpeningHour = buildingOpeningHour
        self.buildingClosingHour = buildingClosingHour


class LocationInfo(BIGGObjects):
    __rdf_type__ = Bigg.LocationInfo

    def __init__(self, subject, addressCountry=None, addressProvince=None, addressCity=None, addressPostalCode=None,
                 addressStreetNumber=None, addressStreetName=None, addressClimateZone=None, addressLongitude=None,
                 addressLatitude=None, addressAltitude=None):
        super().__init__(subject)
        self.addressCountry = addressCountry
        self.addressProvince = addressProvince
        self.addressCity = addressCity
        self.addressPostalCode = addressPostalCode
        self.addressStreetNumber = addressStreetNumber
        self.addressStreetName = addressStreetName
        self.addressClimateZone = addressClimateZone
        self.addressLongitude = addressLongitude
        self.addressLatitude = addressLatitude
        self.addressAltitude = addressAltitude


class CadastralInfo(BIGGObjects):
    __rdf_type__ = Bigg.CadastralInfo

    def __init__(self, subject, landCadastralReference=None, landGeometry=None, landArea=None,
                 landGraphicalArea=None, landLocation=None, landType=None, landPropertyClass=None):
        super().__init__(subject)
        self.landCadastralReference = landCadastralReference
        self.landGeometry = landGeometry
        self.landArea = landArea
        self.landGraphicalArea = landGraphicalArea
        self.landLocation = landLocation
        self.landType = landType
        self.landPropertyClass = landPropertyClass


class BuildingSpace(BIGGObjects):
    __rdf_type__ = Bigg.BuildingSpace

    def __init__(self, subject, buildingSpaceName=None, buildingSpaceUseType=None):
        super().__init__(subject)
        self.buildingSpaceName = buildingSpaceName
        self.buildingSpaceUseType = buildingSpaceUseType


class Area(BIGGObjects):
    __rdf_type__ = Bigg.Area

    def __init__(self, subject, areaType=None, areaValue=None, areaUnitOfMeasurement=None):
        super().__init__(subject)
        self.areaType = areaType
        self.areaValue = areaValue
        self.areaUnitOfMeasurement = areaUnitOfMeasurement


class BuildingConstructionElement(BIGGObjects):
    __rdf_type__ = Bigg.BuildingConstructionElement

    def __init__(self, subject, buildingElementState=None, buildingElementPurchaseDate=None,
                 buildingElementInstallationDate=None, buildingElementBrand=None,
                 buildingElementModel=None, buildingElementSerialNumber=None,
                 buildingElementManufacturer=None, buildingElementManufactureDate=None,
                 buildingConstructionElementType=None):
        super().__init__(subject)
        self.buildingElementState = buildingElementState
        self.buildingElementPurchaseDate = buildingElementPurchaseDate
        self.buildingElementInstallationDate = buildingElementInstallationDate
        self.buildingElementBrand = buildingElementBrand
        self.buildingElementModel = buildingElementModel
        self.buildingElementSerialNumber = buildingElementSerialNumber
        self.buildingElementManufacturer = buildingElementManufacturer
        self.buildingElementManufactureDate = buildingElementManufactureDate
        self.buildingConstructionElementType = buildingConstructionElementType


class Device(BIGGObjects):
    __rdf_type__ = Bigg.Device
    def __init__(self, subject, deviceName=None, deviceType=None,
                 deviceManufacturer=None, deviceModel=None,
                 deviceNumberOfOutputs=None, deviceElectricSupply=None,
                 deviceOperatingSystem=None, deviceLicenseVersionNumber=None,
                 deviceInputSignalType=None, inputProtocol=None):
        super().__init__(subject)
        self.deviceName = deviceName
        self.deviceType = deviceType
        self.deviceManufacturer = deviceManufacturer
        self.deviceModel = deviceModel
        self.deviceNumberOfOutputs = deviceNumberOfOutputs
        self.deviceElectricSupply = deviceElectricSupply
        self.deviceOperatingSystem = deviceOperatingSystem
        self.deviceLicenseVersionNumber = deviceLicenseVersionNumber
        self.deviceInputSignalType = deviceInputSignalType
        self.inputProtocol = inputProtocol


class MeasurementList(BIGGObjects):
    __rdf_type__ = Bigg.MeasurementList
    def __init__(self, subject, measurementUnit=None, measuredProperty=None,
                 measurementDescription=None, measurementReadingType=None,
                 measurementTypeForEnergy=None, measurementSourceForEnergy=None,
                 outputProtocol=None, outputSignalType=None):
        super().__init__(subject)
        self.measurementUnit = measurementUnit
        self.measuredProperty = measuredProperty
        self.measurementDescription = measurementDescription
        self.measurementReadingType = measurementReadingType
        self.measurementTypeForEnergy = measurementTypeForEnergy
        self.measurementSourceForEnergy = measurementSourceForEnergy
        self.outputProtocol = outputProtocol
        self.outputSignalType = outputSignalType
