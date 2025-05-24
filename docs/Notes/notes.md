---
  title: Notes
---

1. The Solr query in the migration uses the cursor query method, and the `cursorMark` from the cursor query is recorded in a migration database and is used for restart purposes.

2. The cursor query needs special permissions to the Solr indexes, i.e., the IP address of the machine in which the cursor query is made needs to be included in the white-lists of the indexes.

3. There are `project=cmip6` and `project=CMIP6`, `project=e3sm` and `project=E3SM`. It found that lower case `cmip6` and upper case `E3SM` were used by the retracted documents, so only the `CMIP6` and `e3sm` were migrated to the Globus indexes.

4. Generally, the size of 500 documents for the CMIP projects is much less than the maximum size (10 MB) for ingestion. However, for `project=e3sm` and `project=e3sm-supplement`, 
the size of **500** documents is larger than 10 MB, so we use `rows=100` in the migration of `e3sm` and `e3sm-supplement` projects, and **1500** for other projects.

5. Changes to the Solr document:
    - change the value of the `index_node` to __us_index__
    - remove the `url` item if the type of document is `Datsets`

6. Filters during the migration:
    - filter out all metadata at the ORNL index if the value of `data_node` does not include `anl.gov` and `llnl.gov` and does not equal `esgf-node.ornl.gov`
    - filter out all metadata at the LLNL index when the `source_id=E3SM-2-1`

7. The metadata of `project=e3sm` at the ORNL index shall be discarded as the metadata was migrated to the ORNL index, but the actual files were not. But half of these metadata have been ingested to the *E3SM* staged index.

8. The formats of the `_timestamp` strings may differ as there are two APIs to generate them during the publications (`YYYY-MM-DDThh:mm:ss.sssZ` vs. `YYYY-MM-DDThh:mm:ssZ`).
    - A legacy API for harvesting thredds catalogs and generating publication records
    - the "push" publisher API (esg-publisher)

9. The solr query used in the migration only searches the default local shard with port 8983, there is another local shard with port 8995 that is only used for GFDL CMIP6 data. {++MX: It is not true anymore. 
There were CMIP5 data in the local shard with port 8995 too. So we migrated the CMIP5 and CMIP5 metadata on the shard. On Apr. 12, 2025, we found that the ==obs4MIPs and other projects== are in the shared too!!!++}

10. The other shards shown in the LLNL esg_search, are the "replica" shards at LLNL and are the synced copies of the remote shards. However, the GFDL's remote shard is dead,
thus, the GFDL local replica shard (port: 8995) could be considered a local shard for migration purposes.

11. The esg search using the LLNL index is the distributed query by default, however, the distributed shards do not include the ANL and ORNL solr index. 
The esg search using the ANL or ORNL indexes only searches their local shards even when `distrib=True`.

12. The Solr query string used in the migration:
    - query the default shard (for ANL, LLNL and ORNL default shard with port 8983)
          ```bash
          select?sort=id+asc&rows=500&cursorMark=*&wt=json&q=project:CMIP6&fq=_timestamp:[* TO 2025-03-16T00:00:00Z]
          ```
    - query the additional local shard (only for LLNL index and the shard is used to save the GFDL metadata)
          ```bash
          select?sort=id+asc&rows=500&cursorMark=*&wt=json&q=project:CMIP6&fq=_timestamp:[* TO 2025-03-16T00:00:00Z]&shards=localhost:8995/solr/datasets
          ```

13. The esg search query string used in the verfication:
    - query the default shard (for ANL, LLNL and ORNL default shard with port 8983)
          ```bash
          search?query=project:CMIP6&limit=500&from=*&to=2025-03-16T00:00:00Z&format=application%2Fsolr%2Bjson&distrib=false
          ```

    - query the additional local shard (only for LLNL index and the shard is used to save the GFDL metadata)
          ```bash
          search?query=project:CMIP6&limit=500&from=*&to=2025-03-16T00:00:00Z&format=application%2Fsolr%2Bjson&distrib=true&shards=localhost:8995/solr
          ```


14. The following metadata are considered as errors or test datasets/files and shall be deleted from the public index:
    - metadata with the data node is `lapdat02-p.gfdl.noaa.gov` (1 dataset and 16 files)
    - metadata with the data node is `esgf-node.cels.anl.gov` ( 1 dataset and 3 files)


15. The master and slave Solr indexes are not the same. The slave Solr index is connected to the `esgf-node.ornl.gov`. They should be synced with each other, but they did not. So we found that:
    - For input4MIPs project, the count of the metadata in the master solr index is less than that in the slave index. (3 datasets and 3 files less)

16. After our first migration on March 31, 2025, there were several retracts on the data node `crd-esgf-drc.ec.gc.ca` and `esgf-data1.llnl.gov`. How do we handle the retractions?

17. ==Synchronizer design:==

    - It is run every 5 minutes under the crontab in a container at ORNL.
    - The query time window is 5 minutes, from the 20 to 15 minutes before the start time of the synchronizer.
    - Only one instance is allowed to run. So if the previous sync does not stop after 5 minutes, the new sync won't start and will wait for another 5 minutes to start till the previous sync ends.
    - The UTC time in the synchronizer is obtained from the internet NTP sever (`pool.ntp.org`) with two web APIs 
      (`http://worldtimeapi.org/api/timezone/Etc/UTC` and `https://timeapi.io/api/Time/current/zone?timeZone=UTC`) and system time as backups.
    - During the test period (now to end of April), the metadata are synced to the backup index (a37bc34d-de15-493b-9221-b95b13114fd8), instead of the public index.
    - If the previous sync fails, the next sync will first restart the previous sync by finding the `marker` (scroll query) or `offset` (post query) in the migration database.
    - The query of the Globus staged indexes is the scroll search as the post search cannot be used in the case of the number of records larger than 10000. We cannot assume that
the number of records in the query of a 5-min sync is less than 10000.
    - The batched ingest is used to make sure that the total size of each batch is less than 10 MB (the maxium size allowed by Globus ingest).
    - the synchronizer will be deployed to the container at ORNL and be converted from test to production runs on May 1, 2025?

18. 
