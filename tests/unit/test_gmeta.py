
from metadata_migrate_sync.gmeta import ModifiedGmetaGenerator
from metadata_migrate_sync.convert import replicate_gmeta
import pytest
import json
from metadata_migrate_sync.globus import GlobusCV



def test_gmeta_skip(gmeta_sample_wrong_type):


    gpage = gmeta_sample_wrong_type


    gm =  ModifiedGmetaGenerator(
        modifier = replicate_gmeta,
        metatype = 'Dataset',
        source_data_node = 'llnl',
        target_data_node = 'ornl',
        has_globus = False,
        is_replica = True,
    )
    gm_list, gm_list_skip = gm.generate(gpage)

    assert len(gm_list_skip[GlobusCV.INGEST_DATA.value][GlobusCV.GMETA.value]) == 1
    assert len(gm_list[GlobusCV.INGEST_DATA.value][GlobusCV.GMETA.value]) == 0
