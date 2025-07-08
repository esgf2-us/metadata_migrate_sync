---
title: Data transfer and metadata replication
---


## Purpose

  1. Find the missing NetCDF files on ORNL data node and transfer them
  2. Replicate all the data on LLNL data nodes to the ORNL data node


## Terms

  - __LLNL metadata__: any metadata in which the value of `data_node` is one of esgf-data1.llnl.gov, esgf-data2.llnl.gov, and aims3.llnl.gov
  - __ORNL metadata__: any metadata in which the value of `data_node` is esgf-node.ornl.gov

## Two phases of data transfer

  - Phase 1: Retrieve all data paths from the ORNL metadata URLs, 
verify their existence on the ORNL data node, and ensure they have non-zero sizes. 
For any missing or zero-size files, locate and transfer them from the LLNL and ANL data nodes.

     - Missing files: __557,560__
     - Successfully transferred/found: __547,123__
     - Paths requiring fix: 46,500
     - Files unavailable at DOE sites: 10,432  

  - Phase 2: Identify all LLNL metadata entries missing in ORNL, extract their data paths, 
and transfer the corresponding files from LLNL to ORNL.

     - Successfully transferred/found: 158,796(CMIP6), 1588(input4MIPs), 5533(DRCDP), 9(CMIP6Plus):
     - Files unavailable at DOE sites: 170


## Results

  - [Phase 1](./phase1.md)

  - [Phase 2](./phase2.md)



