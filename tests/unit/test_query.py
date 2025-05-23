import pytest
import requests
import responses

from metadata_migrate_sync.database import MigrationDB
from metadata_migrate_sync.project import ProjectReadOnly
from metadata_migrate_sync.query import SolrQuery, params_search

cmip6_cusormark_list_row10_idasc_ornl = [
'AoE/b0NNSVA2LkFlckNoZW1NSVAuQkNDLkJDQy1FU00xLnNzcDM3MC5yMWkxcDFmMS5BbW9uLnBzLmduLnYyMDE5MDYyNC5wc19BbW9uX0JDQy1FU00xX3NzcDM3MF9yMWkxcDFmMV9nbl8yMDE1MDEtMjA1NTEyLm5jfGVzZ2YtZGF0YTA0LmRpYXNqcC5uZXQ=',
'AoE/c0NNSVA2LkFlckNoZW1NSVAuQkNDLkJDQy1FU00xLnNzcDM3MC5yMWkxcDFmMS5BbW9uLnJzdXMuZ24udjIwMTkwNjI0LnJzdXNfQW1vbl9CQ0MtRVNNMV9zc3AzNzBfcjFpMXAxZjFfZ25fMjAxNTAxLTIwNTUxMi5uY3xlc2dmLWRhdGEwNC5kaWFzanAubmV0',
'AoE/b0NNSVA2LkFlckNoZW1NSVAuQkNDLkJDQy1FU00xLnNzcDM3MC5yMWkxcDFmMS5BbW9uLnRzLmduLnYyMDE5MDYyNC50c19BbW9uX0JDQy1FU00xX3NzcDM3MF9yMWkxcDFmMV9nbl8yMDE1MDEtMjA1NTEyLm5jfGVzZ2YtZGF0YTA0LmRpYXNqcC5uZXQ=',
'AoE/c0NNSVA2LkFlckNoZW1NSVAuQkNDLkJDQy1FU00xLnNzcDM3MC5yMWkxcDFmMS5MSW1vbi50c24uZ24udjIwMTkwNjAxLnRzbl9MSW1vbl9CQ0MtRVNNMV9zc3AzNzBfcjFpMXAxZjFfZ25fMjAxNTAxLTIwNTUxMi5uY3xlc2dmLWRhdGEwNC5kaWFzanAubmV0',
'AoE/a0NNSVA2LkFlckNoZW1NSVAuQkNDLkJDQy1FU00xLnNzcDM3MC5yMWkxcDFmMS5PZngudm9sY2VsbG8uZ24udjIwMjAxMDIxLnZvbGNlbGxvX09meF9CQ0MtRVNNMV9zc3AzNzBfcjFpMXAxZjFfZ24ubmN8ZXNnZi1kYXRhMDQuZGlhc2pwLm5ldA==',
'AoE/d0NNSVA2LkFlckNoZW1NSVAuQkNDLkJDQy1FU00xLnNzcDM3MC5yMWkxcDFmMS5PbW9uLnRoZXRhby5nbi52MjAxOTA2MjQudGhldGFvX09tb25fQkNDLUVTTTFfc3NwMzcwX3IxaTFwMWYxX2duXzIwNTUwMS0yMDU1MTIubmN8ZXNnZi1kYXRhMDQuZGlhc2pwLm5ldA==',
'AoE/c0NNSVA2LkFlckNoZW1NSVAuQkNDLkJDQy1FU00xLnNzcDM3MC5yMWkxcDFmMS5kYXkuaHVzLmduLnYyMDE5MDYyNC5odXNfZGF5X0JDQy1FU00xX3NzcDM3MF9yMWkxcDFmMV9nbl8yMDE1MDEwMS0yMDM0MTIzMS5uY3xlc2dmLWRhdGEwNC5kaWFzanAubmV0',
'AoE/dUNNSVA2LkFlckNoZW1NSVAuQkNDLkJDQy1FU00xLnNzcDM3MC5yMWkxcDFmMS5kYXkucnN1cy5nbi52MjAxOTA2MjQucnN1c19kYXlfQkNDLUVTTTFfc3NwMzcwX3IxaTFwMWYxX2duXzIwMTUwMTAxLTIwNTUxMjMxLm5jfGVzZ2YtZGF0YTA0LmRpYXNqcC5uZXQ=',
'AoE/cUNNSVA2LkFlckNoZW1NSVAuQkNDLkJDQy1FU00xLnNzcDM3MC5yMWkxcDFmMS5kYXkudWEuZ24udjIwMTkwNjI0LnVhX2RheV9CQ0MtRVNNMV9zc3AzNzBfcjFpMXAxZjFfZ25fMjA1NTAxMDEtMjA1NTEyMzEubmN8ZXNnZi1kYXRhMDQuZGlhc2pwLm5ldA==',
'AoE/cUNNSVA2LkFlckNoZW1NSVAuQkNDLkJDQy1FU00xLnNzcDM3MC5yMWkxcDFmMS5kYXkuemcuZ24udjIwMTkwNjI0LnpnX2RheV9CQ0MtRVNNMV9zc3AzNzBfcjFpMXAxZjFfZ25fMjAzNTAxMDEtMjA1NDEyMzEubmN8ZXNnZi1kYXRhMDQuZGlhc2pwLm5ldA=='
]

e3smsuppl_cursormark_list_row10_idasc_ornl = [
'AoE/b0NNSVA2LkFlckNoZW1NSVAuQkNDLkJDQy1FU00xLnNzcDM3MC5yMWkxcDFmMS5BbW9uLnBzLmduLnYyMDE5MDYyNC5wc19BbW9uX0JDQy1FU00xX3NzcDM3MF9yMWkxcDFmMV9nbl8yMDE1MDEtMjA1NTEyLm5jfGVzZ2YtZGF0YTA0LmRpYXNqcC5uZXQ=',
'AoE/c0NNSVA2LkFlckNoZW1NSVAuQkNDLkJDQy1FU00xLnNzcDM3MC5yMWkxcDFmMS5BbW9uLnJzdXMuZ24udjIwMTkwNjI0LnJzdXNfQW1vbl9CQ0MtRVNNMV9zc3AzNzBfcjFpMXAxZjFfZ25fMjAxNTAxLTIwNTUxMi5uY3xlc2dmLWRhdGEwNC5kaWFzanAubmV0',
'AoE/b0NNSVA2LkFlckNoZW1NSVAuQkNDLkJDQy1FU00xLnNzcDM3MC5yMWkxcDFmMS5BbW9uLnRzLmduLnYyMDE5MDYyNC50c19BbW9uX0JDQy1FU00xX3NzcDM3MF9yMWkxcDFmMV9nbl8yMDE1MDEtMjA1NTEyLm5jfGVzZ2YtZGF0YTA0LmRpYXNqcC5uZXQ=',
'AoE/c0NNSVA2LkFlckNoZW1NSVAuQkNDLkJDQy1FU00xLnNzcDM3MC5yMWkxcDFmMS5MSW1vbi50c24uZ24udjIwMTkwNjAxLnRzbl9MSW1vbl9CQ0MtRVNNMV9zc3AzNzBfcjFpMXAxZjFfZ25fMjAxNTAxLTIwNTUxMi5uY3xlc2dmLWRhdGEwNC5kaWFzanAubmV0',
'AoE/a0NNSVA2LkFlckNoZW1NSVAuQkNDLkJDQy1FU00xLnNzcDM3MC5yMWkxcDFmMS5PZngudm9sY2VsbG8uZ24udjIwMjAxMDIxLnZvbGNlbGxvX09meF9CQ0MtRVNNMV9zc3AzNzBfcjFpMXAxZjFfZ24ubmN8ZXNnZi1kYXRhMDQuZGlhc2pwLm5ldA==',
'AoE/d0NNSVA2LkFlckNoZW1NSVAuQkNDLkJDQy1FU00xLnNzcDM3MC5yMWkxcDFmMS5PbW9uLnRoZXRhby5nbi52MjAxOTA2MjQudGhldGFvX09tb25fQkNDLUVTTTFfc3NwMzcwX3IxaTFwMWYxX2duXzIwNTUwMS0yMDU1MTIubmN8ZXNnZi1kYXRhMDQuZGlhc2pwLm5ldA==',
'AoE/c0NNSVA2LkFlckNoZW1NSVAuQkNDLkJDQy1FU00xLnNzcDM3MC5yMWkxcDFmMS5kYXkuaHVzLmduLnYyMDE5MDYyNC5odXNfZGF5X0JDQy1FU00xX3NzcDM3MF9yMWkxcDFmMV9nbl8yMDE1MDEwMS0yMDM0MTIzMS5uY3xlc2dmLWRhdGEwNC5kaWFzanAubmV0',
'AoE/dUNNSVA2LkFlckNoZW1NSVAuQkNDLkJDQy1FU00xLnNzcDM3MC5yMWkxcDFmMS5kYXkucnN1cy5nbi52MjAxOTA2MjQucnN1c19kYXlfQkNDLUVTTTFfc3NwMzcwX3IxaTFwMWYxX2duXzIwMTUwMTAxLTIwNTUxMjMxLm5jfGVzZ2YtZGF0YTA0LmRpYXNqcC5uZXQ=',
'AoE/cUNNSVA2LkFlckNoZW1NSVAuQkNDLkJDQy1FU00xLnNzcDM3MC5yMWkxcDFmMS5kYXkudWEuZ24udjIwMTkwNjI0LnVhX2RheV9CQ0MtRVNNMV9zc3AzNzBfcjFpMXAxZjFfZ25fMjA1NTAxMDEtMjA1NTEyMzEubmN8ZXNnZi1kYXRhMDQuZGlhc2pwLm5ldA==',
'AoE/cUNNSVA2LkFlckNoZW1NSVAuQkNDLkJDQy1FU00xLnNzcDM3MC5yMWkxcDFmMS5kYXkuemcuZ24udjIwMTkwNjI0LnpnX2RheV9CQ0MtRVNNMV9zc3AzNzBfcjFpMXAxZjFfZ25fMjAzNTAxMDEtMjA1NDEyMzEubmN8ZXNnZi1kYXRhMDQuZGlhc2pwLm5ldA=='
]



def test_query_cursormark():
    if "CMIP6" in ProjectReadOnly._value2member_map_:
        params_search = {
              "q": "project:CMIP6",
              "sort": "id asc",
              "limit": 10,
              "cursorMark": "*",
              "wt": "json",
        }

        index_url = "http://127.0.0.1:8983/solr/files/select"
        mark_list = []
        for req in range(0, 10):
            response = requests.get(index_url, params=params_search)
            if response.status_code == requests.codes.ok:
                print (response.elapsed.total_seconds())
                res_json = response.json()
                mark_list.append(res_json.get("nextCursorMark"))
                params_search["cursorMark"] = res_json.get("nextCursorMark")

        assert mark_list == cmip5_cusormark_list_row10_idasc_ornl

def test_query_cursormark_different_rows():
    if "CMIP6" in ProjectReadOnly._value2member_map_:
        params_search = {
              "q": "project:CMIP6",
              "sort": "id asc",
              "rows": 5,
              "cursorMark": "*",
              "wt": "json",
              "fq": "_timestamp:[* TO 2025-03-16T00:00:00Z]",
        }

        index_url = "http://127.0.0.1:8983/solr/files/select"

        params_search["rows"] = 5
        mark_list_5 = []
        for req in range(0, 12):
            response = requests.get(index_url, params=params_search)
            if response.status_code == requests.codes.ok:
                res_json = response.json()
                mark_list_5.append(res_json.get("nextCursorMark"))
                params_search["cursorMark"] = res_json.get("nextCursorMark")

        params_search["rows"] = 10
        params_search["cursorMark"] = "*"
        mark_list_10 = []
        for req in range(0, 6):
            response = requests.get(index_url, params=params_search)
            if response.status_code == requests.codes.ok:
                res_json = response.json()
                mark_list_10.append(res_json.get("nextCursorMark"))
                params_search["cursorMark"] = res_json.get("nextCursorMark")

        params_search["rows"] = 15
        params_search["cursorMark"] = "*"
        mark_list_15 = []
        for req in range(0, 4):
            response = requests.get(index_url, params=params_search)
            if response.status_code == requests.codes.ok:
                res_json = response.json()
                mark_list_15.append(res_json.get("nextCursorMark"))
                params_search["cursorMark"] = res_json.get("nextCursorMark")

        assert mark_list_5[1::2] == mark_list_10[:]
        assert mark_list_5[2::3] == mark_list_15[:]


def test_query():

    params_search = {
          "q": "project:CMIP3",
          "sort": "id asc",
          "limit": 2,
          "cursorMark": "*",
          "wt": "json",
    }

    sq = SolrQuery(
        end_point = "http://127.0.0.1:8983/solr/files/select",
        ep_type = "solr",
        ep_name = "ORNL solr index",
        project = ProjectReadOnly.CMIP3,
        query = params_search
    )

    mdb = MigrationDB("test.sqlite", True)

    n = 0
    for docs in sq.run():

         n = n + 1
         for doc in docs:
            assert 'id' in doc

         if n > 0:
            break


@responses.activate
def test_query_retry_on_server_error():
    url = "http://example.com"

    responses.add(responses.GET, url, status=500)
    responses.add(responses.GET, url, status=500)

    responses.add(responses.GET, url, status=200)

    status = SolrQuery._make_request(url, params_search, True)

    assert status == 200

    assert len(responses.calls) == 3


@responses.activate
def test_query_retry_on_connection_error():
    url = "http://example.com"

    responses.add(responses.GET, url, body=ConnectionError("Connection error"))
    responses.add(responses.GET, url, status=200)


    with pytest.raises(Exception):
        status = SolrQuery._make_request(url, params_search, True)
    status = SolrQuery._make_request(url, params_search, True)

    assert status == 200
    assert len(responses.calls) == 2

@responses.activate
def test_query_exhaust_retries():
    url = "http://example.com"

    for _ in range(3):
        responses.add(responses.GET, url, status=500)

    #with pytest.raises(urllib3.exceptions.MaxRetryError):
    with pytest.raises(requests.exceptions.RetryError):
        SolrQuery._make_request(url, params_search, True)

    assert len(responses.calls) == 4


