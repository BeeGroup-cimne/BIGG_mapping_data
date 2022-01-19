from rdflib.term import URIRef
from rdflib.namespace import DefinedNamespace, Namespace


class Bigg(DefinedNamespace):
    """
    BIGG Ontology classes, properties and entities.

    Generated from: ../schemas/datatypes.xsd
    Date: 2021-09-05 20:37+10

    """
    _NS = Namespace("https://bigg-project.eu/ontology#")
    _warn = False

    Organization: URIRef
    organizationName: URIRef
    organizationType: URIRef
    organizationDivisionType: URIRef
    organizationContactPersonName: URIRef
    organizationEmail: URIRef
    organizationTelephoneNumber: URIRef
    hasSuperOrganization: URIRef
    hasSubOrganization: URIRef
    managesBuilding: URIRef

    Building: URIRef
    buildingIDFromOrganization: URIRef
    buildingName: URIRef
    buildingConstructionYear: URIRef
    buildingConstructionType: URIRef
    buildingUseType: URIRef
    buildingOwnership: URIRef
    buildingOpeningHour: URIRef
    buildingClosingHour: URIRef
    pertainsToOrganization: URIRef
    hasLocationInfo: URIRef
    hasCadastralInfos: URIRef
    hasSpace: URIRef
    hasEPCs: URIRef

    LocationInfo: URIRef
    addressCountry: URIRef
    addressProvince: URIRef
    addressCity: URIRef
    addressPostalCode: URIRef
    addressStreetName: URIRef
    addressStreetNumber: URIRef
    addressClimateZone: URIRef
    addressLongitude: URIRef
    addressLatitude: URIRef
    addressAltitude: URIRef

    CadastralInfo: URIRef
    landCadastralReference: URIRef
    landGeometry: URIRef
    landArea: URIRef
    landGraphicalArea: URIRef
    landLocation: URIRef
    landType: URIRef
    landPropertyClass: URIRef

    BuildingSpace: URIRef
    buildingSpaceName: URIRef
    buildingSpaceUseType: URIRef
    hasAreas: URIRef
    hasSubSpaces: URIRef
    hasSuperSpaces: URIRef
    observesElements: URIRef
    isAssociatedWithElements: URIRef
    containsElement: URIRef
    hasUtilityPointOfDelivery: URIRef


    Area: URIRef
    areaType: URIRef
    areaValue: URIRef
    areaUnitOfMeasurement: URIRef

    BuildingConstructionElement: URIRef
    buildingElementID: URIRef
    buildingElementState: URIRef
    buildingElementPurchaseDate: URIRef
    buildingElementInstallationDate: URIRef
    buildingElementBrand: URIRef
    buildingElementModel: URIRef
    buildingElementSerialNumber: URIRef
    buildingElementManufacturer: URIRef
    buildingElementManufactureDate: URIRef
    buildingConstructionElementType: URIRef

    Device: URIRef
    deviceName: URIRef
    deviceType: URIRef
    deviceManufacturer: URIRef
    deviceModel: URIRef
    deviceNumberOfOutputs: URIRef
    deviceElectricSupply: URIRef
    deviceOperatingSystem: URIRef
    deviceLicenseVersionNumber: URIRef
    deviceInputSignalType: URIRef
    inputProtocol: URIRef
    observesSpaces: URIRef
    observesElements: URIRef
    isPartOfModelingUnit: URIRef
    isInWeatherStation: URIRef
    hasMeasurementLists: URIRef

    isAffectedByMeasures: URIRef
    isContainedInSpaces: URIRef
    isAssociatedWithSpaces: URIRef
    hasSubElement: URIRef
    hasSuperElement: URIRef
    hasSuperElement: URIRef
    isObservedBy: URIRef

    UtilityPointOfDelivery: URIRef
    pointOfDeliveryIDFromUser: URIRef
    utilityType: URIRef
    hasDevice: URIRef

    DeviceAggregator: URIRef
    deviceAggregatorFormula: URIRef
    includesDevices: URIRef

    EnergyEfficiencyMeasure: URIRef
    energyEfficiencyMeasureType: URIRef
    energyEfficiencyMeasureDescription: URIRef
    shareOfAffectedElement: URIRef
    energyEfficiencyMeasureOperationalDate: URIRef
    energyEfficiencyMeasureInvestment: URIRef
    energyEfficiencyMeasureInvestmentCurrency: URIRef
    energyEfficiencyMeasureCurrencyExchangeRate: URIRef
    energyEfficiencyMeasureSavingsToInvestmentRatio: URIRef
    energySourcePriceEscalationRate: URIRef
    affectsElements: URIRef

    WeatherStation: URIRef
    weatherStationCoordinates: URIRef
    weatherStationType: URIRef
    weatherStationStartDate: URIRef
    weatherStationEndDate: URIRef
    weatherStationTimeStep: URIRef

    MeasurementList: URIRef
    measurementUnit: URIRef
    measuredProperty: URIRef
    measurementDescription: URIRef
    measurementReadingType: URIRef
    measurementTypeForEnergy: URIRef
    measurementSourceForEnergy: URIRef
    outputProtocol: URIRef
    outputSignalType: URIRef
