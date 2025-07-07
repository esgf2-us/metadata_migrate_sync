
## Two steps to transfer data from LLNL to ORNL

### Purpose

  1. Find the missing NetCDF files on ORNL data node and transfer them
  2. Replicate all the data on LLNL data nodes to the ORNL data node


### Terms

LLNL metadata: any metadata in which the value of `data_node` is one of esgf-data1.llnl.gov, esgf-data2.llnl.gov, and aims3.llnl.gov
ORNL metadata: any metadata in which the value of `data_node` is esgf-node.ornl.gov

### Two phase data transfer

  - Phase 1: Retrieve all data paths in the URLs of ORNL metadata and check if they exist on the ORNL data node and their sizes are not zero, if missing, find and transfer them from LLNL and ANL data nodes.

     - transferred: 
     - paths need to fix
     - missing:  

  - Phase 2: Find all LLNL metadata that ORNL does not have and retrieve their paths and transfer them from LLNL to ORNL

     - transferred:
     - paths need to fix
     - missing:


### Results

  - [Phase 1](./phase1.md)

  - [Phase 2](./phase2.md)



