---
title: Project verification
---


## Contributions of three Solr index to the public index for each project
{%
  include "Migration/jsons/chart_project.md"
%}

## Tables of counts of documents in the Solr and public indexes

!!! tip "Caption"
    - `numFound`: the total number of documents from a Solr query
    - `Total records`: the total number of documents to be ingested from the database files
    - `skipped records`: the number of documents skipped (see Notes)
    - `Ingested`: the number of documents to be ingested successfully. Generally it equals to the `Total records-skipped records-failed ingested` 
    - `Entries`: the number of documents in the public index using the globus query
{%
  include "Migration/jsons/table_project.md"
%}
