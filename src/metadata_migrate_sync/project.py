from enum import Enum

# change.md #3a78b9f
# The index will only contain metadata for the
# CMIP3, CMIP5, CMIP6, CREATE-IP, DRCDP, E3SM, E3SM-supplement,
# GeoMIP, input4MIPS2, LUCID, obs4MIPs, and TAMIP projects

# CMIP6 was moved to readonly project in the meeting note
# https://docs.google.com/document/d/1ajruy9E6qneOK5y8ijpERYIB8zYUbldfWBBhIZwAkGM/edit?tab=t.0
#


class ProjectReadOnly(str, Enum):
    """read-only projects"""

    CMIP3 = "CMIP3"
    CMIP5 = "CMIP5"
    CMIP6 = "CMIP6"
    CREATETP = "CREATE-IP"
    E3SMSUPPL = "e3sm-supplement"
    GEOMIP = "GeoMIP"
    LUCID = "LUCID"
    TAMIP = "TAMIP"
    GFDL_CMIP6 = "GFDL-CMIP6"
    GFDL_CMIP5 = "GFDL-CMIP5"
    GFDL_LLNL_CMIP6 = "GFDL-LLNL-CMIP6"
    GFDL_LLNL_CMIP5 = "GFDL-LLNL-CMIP5"


class ProjectReadWrite(str, Enum):
    """read-write projects"""

    CMIP6PLUS = "CMIP6Plus"
    DRCDP = "DRCDP"
    E3SM = "e3sm"
    INPUT4MIPS = "input4MIPs"
    OBS4MIPS = "obs4MIPs"
