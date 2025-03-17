from enum import Enum



class ProjectReadOnly(str, Enum):
    """read-only projects"""
    CMIP3 = "CMIP3"
    CMIP5 = "CMIP5"
    CREATETP = "CREATE-IP"
    E3SMSUPPL = "E3SM-supplement"
    GEOMIP = "GeoMIP"
    LUCID = "LUCID"
    TAMIP = "TAMIP"

class ProjectReadWrite(str, Enum):
    """read-write projects"""
    CMIP6 = "CMIP6" 
    CMIP6PLUS = "CMIP6Plus"
    DRCDP = "DRCDP"
    E3SM = "E3SM"
    INPUT4MIPS = "input4MIPs"
    OBS4MIPS = "obs4MIPs"
