import pytest
import json

from metadata_migrate_sync.ingest import GlobusIngest, generate_gmeta_list

from metadata_migrate_sync.globus import GlobusClient

from metadata_migrate_sync.project import ProjectReadOnly

from metadata_migrate_sync.database import MigrationDB, Ingest, Query, Datasets, Files

@pytest.fixture
def solr_dataset(datadir, request):
    with open(datadir / f"dataset_solr_facet_{request.param}.json" ) as fh:
        content = json.loads(fh.read())

    return content

@pytest.fixture
def solr_file(datadir, request):
    with open(datadir / f"file_solr_facet_{request.param}.json" ) as fh:
        content = json.loads(fh.read())

    return content


@pytest.mark.parametrize("solr_dataset", ["cmip3", "cmip5", "cmip6"], indirect=True)
def test_solr_dataset(request, solr_dataset, snapshot):

    strid = str(request.node.name)
    cm = GlobusClient.get_client(name = "test")


    r = cm.list_index()
    assert "test" in r
    assert "52eff156-6141-4fde-9efe-c08c92f3a706" in r["test"]["id"]

    gmeta_list = generate_gmeta_list(solr_dataset.get("response").get("docs")[0:1], "datasets")

    snapshot.assert_match(json.dumps(gmeta_list, indent=4), f"test_ingest_dataset_gmeta_list{strid}.json")




@pytest.mark.parametrize("solr_file", ["cmip3", "cmip5", "cmip6"], indirect=True)
def test_solr_file(request, solr_file, snapshot):

    strid = str(request.node.name)
    cm = GlobusClient.get_client(name = "test")
    r = cm.list_index()
    assert "test" in r
    assert "52eff156-6141-4fde-9efe-c08c92f3a706" in r["test"]["id"]

    gmeta_list = generate_gmeta_list(solr_file.get("response").get("docs")[0:1], "files")

    snapshot.assert_match(json.dumps(gmeta_list, indent=4), f"test_ingest_file_gmeta_list{strid}.json")




@pytest.mark.parametrize("solr_dataset", ["cmip3"], indirect=True)
def test_ingest_submitted(solr_dataset):
    gmeta_list = generate_gmeta_list(solr_dataset.get("response").get("docs")[0:1], "datasets")

    cm = GlobusClient.get_client(name = "test")
    r = cm.list_index()
    gi = GlobusIngest(
        end_point=r["test"]["id"],
        ep_name="test",
        project=ProjectReadOnly.CMIP3,
    )

    #gi.ingest(gmeta_list)

    #assert gi._submitted == True
    assert gi._submitted == False



@pytest.fixture
def query_dummy():
    
    query = Query(
        project = "CMIP5", 
        project_type = "readonly", 

        index_id = "http://127.0.0.1:8983", 

        query_str = "sort=id+asc&rows=2&cursorMark=%2A&wt=json&q=project%3ACMIP3", 

        query_type = "solr", 
        query_time = 1.09, 
        date_range = '[* TO *]', 
        numFound = 88888888, 
        n_datasets = 0, 
        n_files = 8888888, 

        pages = 1, 
        rows = 2, 
        cursorMark = "*", 
        cursorMark_next = "xxx", 
        n_failed = 0, 
    )


    return query

@pytest.mark.parametrize("solr_file", ["cmip5"], indirect=True)
def test_ingest_prov_collect(query_dummy, solr_file, datadir):


    ingest_ver = {'n_ingested': 1, 'n_datasets': 0, 'n_files': 1, 'index_id': '52eff156-6141-4fde-9efe-c08c92f3a706', 
        'task_id': 'a04ae23d-6fd4-42af-b52c-d54577db97dc', 
        'ingest_response': '{"acknowledged": true, "task_id": "a04ae23d-6fd4-42af-b52c-d54577db97dc", "success": true, "num_documents_ingested": 0}', 
        'submitted': 1}

    files_ver = {'source_index': 'http://127.0.0.1:8983', 'target_index': '52eff156-6141-4fde-9efe-c08c92f3a706', 'files_id': 
        'cmip5.output.CCCma.CanAM4.amip.3hr.atmos.r3i1p1.v20130331.sfcWind_cf3hr_CanAM4_amip_r3i1p1_197901010300-201001010000.nc|crd-esgf-drc.ec.gc.ca', 
        'uri': 'http://crd-esgf-drc.ec.gc.ca/thredds/fileServer/esg_dataroot/AR5/CMIP5/output/CCCma/CanAM4/amip/3hr/atmos/sfcWind/r3i1p1/sfcWind_cf3hr_CanAM4_amip_r3i1p1_197901010300-201001010000.nc|application/netcdf|HTTPServer,gsiftp://crd-esgf-drc.ec.gc.ca:2811//esg_dataroot/AR5/CMIP5/output/CCCma/CanAM4/amip/3hr/atmos/sfcWind/r3i1p1/sfcWind_cf3hr_CanAM4_amip_r3i1p1_197901010300-201001010000.nc|application/gridftp|GridFTP,http://crd-esgf-drc.ec.gc.ca/thredds/dodsC/esg_dataroot/AR5/CMIP5/output/CCCma/CanAM4/amip/3hr/atmos/sfcWind/r3i1p1/sfcWind_cf3hr_CanAM4_amip_r3i1p1_197901010300-201001010000.nc.html|application/opendap-html|OPENDAP', 
         'success': 0}


    gmeta_list = generate_gmeta_list(solr_file.get("response").get("docs")[0:1], "files")

    mdb = MigrationDB(db_filename= datadir / "test.sqlite", insert_index=True)

    print (datadir / "test.sqlite")

    cm = GlobusClient.get_client(name = "test")
    r = cm.list_index()
    gi = GlobusIngest(
        end_point=r["test"]["id"],
        ep_name="test",
        project=ProjectReadOnly.CMIP5,
    )
    
    gi._submitted = True
    gi._response_data = {
        "acknowledged": True,
        "task_id": "a04ae23d-6fd4-42af-b52c-d54577db97dc",
        "success": True,
        "num_documents_ingested": 0,
    }

    gi.prov_collect(gmeta_list, review = False, current_query = query_dummy)


    with MigrationDB.get_session() as session:
        inject_in_db = session.query(Ingest).order_by(Ingest.id.desc()).first()

        inject_in_db_dict = inject_in_db.__dict__
        inject_in_db_dict.pop('_sa_instance_state', None)
        inject_in_db_dict.pop('query', None)
        inject_in_db_dict.pop('pages', None)
        inject_in_db_dict.pop('id', None)
        inject_in_db_dict.pop('ingest_time', None)
        inject_in_db_dict.pop('succeeded', None)
        inject_in_db_dict.pop('n_failed', None)

        files_in_db = session.query(Files).order_by(Files.id.desc()).first()

        files_in_db_dict = files_in_db.__dict__
        files_in_db_dict.pop('_sa_instance_state', None)
        files_in_db_dict.pop('pages', None)
        files_in_db_dict.pop('id', None)
        
        print (files_in_db_dict)
        print (files_ver)

        assert inject_in_db_dict == ingest_ver
        assert files_in_db_dict == files_ver



#-#mdb.init_index()
#-GlobusMeta.model_validate(test_entries[0])
#-m = GlobusIngest(**test_gmeta)
#-
#-
#-ig.prov_collect(test_gmeta, mdb)
#-
#-# print (m.model_dump())
#-
#-# ig.ingest(m.model_dump())
#-#ig.ingest(test_gmeta)
#-
#-
#-# print (test_entries)
#-# print (test_gmeta)
#-
#-# need a filter function
#-
#-gmeta = GlobusMeta(id="file", subject="solr_id", content={"content": "solr_conent"})
#-
#-
#-# print (gmeta)
