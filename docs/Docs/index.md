
# Documents and User Guide

## [ESGF1.5 index toolkit (a.k.a `metadata_migrate_sync`)](./toolkit.md)

## How to restart synchronizer and replicators cron jobs on OpenDTN

### Basic information
To start the ESGF 1.5 synchronizer and replicator cron jobs, users must:

- be a member of the `cli137-open` project and have access to OpenDTN and the themis project shared folder (`/nl/themis/esgf/cli137/proj-shared`);
- possess a Globus client credential with read/write permission for the stage and public indexes. The credential should be stored in `$HOME/.ssh/env_client_secret.sh` with the following content:
  ```bash
  #!/usr/bin/env bash
  export GLOBUS_CLIENT_ID="YOUR_CLIENT_ID"
  export GLOBUS_CLIENT_SECRET="YOUR_CLIENT_SECRET"
  ```
- pass the two tests mentioned in the section [Tests](#tests)

After configuring the Globus client credentials, the remaining steps are:

- log in to one of the OpenDTN front nodes (`opendtn121`, `opendtn122`, or `opendtn123`.ccs.ornl.gov). Note: there should be exactly __one cron job per node__—one for the synchronizer and two for the replicator. If these nodes are unavailable, `opendtn124` or `opendtn125` may be used, though they are typically reserved for submitted jobs;
- navigate to `/nl/themis/esgf/cli137/proj-shared/mfx/ESGF-1-5-Services/crontab` and execute the appropriate script on each front node:
  - `set_crontab_ornl_anl.sh`
  - `set_crontab_ornl_nersc.sh`
  - `set_crontab_sync.sh`

Run `crontab -l` before executing the above command to verify that no cron jobs are already scheduled on the node. Users can also determine which services are running on each node by inspecting the `host*.log` files in that directory.

!!! info

    If not all three OpenDTN front nodes are avaiable, either way as follow can be taken:
        - just run sync on the available node `set_crontab_sync.sh`, and then run the replicators when all three nodes are avaviable
        - run `set_crontab_combined.sh` on the available node. It combines all cron jobs in sync and replicators and runs all of them on one node

!!! note

    Because the replicator transfers data from NERSC or ANL to ORNL using Globus, it requires Globus authorization for OpenDTN via the user's cli137 account, and this authorization expires after three days. Therefore, users must re‑authorize the Globus transfer session whenever data transfers are needed and the globus session is expired. Users typically receive emails with a subject such as "prod‑nersc‑ornl‑stage_{project}_nersc.gov_YYYY‑MM‑DD_00" and the following message body:
    ```
      The resource you are trying to access requires you to re-authenticate.
      message: Session reauthentication required (Globus Transfer)
   
      Please run:
   
         globus session update opensso.ccs.ornl.gov clients.auth.globus.org
   
      to re-authenticate with the required identities. 
    ```
    
    Just use the `cli137` credential to re-authenticate the globus session.

### Tests

There are two test scripts in the `script/tests` directory for the Replicator:

1. `test_globus_transfer.sh`: verifies that the user has write permission in the `cli137` project shared directory. The script initiates a dummy Globus transfer, returns a transfer ID, and reports whether the transfer completes successfully.
2. `test_index_permission.sh`: checks that the user has write access to the ESGF 1.5 indexes.

## Source code and scripts

The synchronizer and replicator source script folders are in the `cli137' project directory (omit it in the following list):

  - synchronizer: `.../proj-shared/mfx/ESGF-1-5-Services/Synchronizer/metadata_migrate_sync_type[^1]`
  - replicator: `.../proj-shared/mfx/ESGF-1-5-Services/Replicator/ornl/scripts` and the 
  `.../proj-shared/mfx/ESGF-1-5-Services/Replicator/ornl/metadata_migrate_sync[^2]`

[^1]: the synchronizer uses the old ESGF1.5 index toolkit (`metadata_migrate_sync_type`)
[^2]: the document of ESGF1.5 index tookit can be referred at [link](./toolkit.md)

### Replicator
#### Hourly tasks

The hourly tasks include:

1. generate the transfer files for globus cli to transfer data to the staged folder on the ORNL data node
2. sync the files in the staged folder to the production folder once the Globus transfers have completed successfully
2. replicate the file and dataset metadata to ORNL in the ESGF1.5 index if the files have been successfuly transferred and synced

##### `query_transfer_files`

The `QUEUE_transfer_${project}_${node}:YYYY-MM_DD` will be moved from `queue_dir` to the `stage_dir`. The globus transfer is batched,
the batched files will be moved to `$backup_dir` after the successful submission of the globus transfer (i.e.,get the transfer ids). 
If not, the trasfer queue file `QUEUE_TRANSFER_${project}_${node}:YYYY-MM_DD` will be moved back to `queue_dir` to 
be queued in next hour. The `TASKID_${project}_${node}:YYYY-MM_DD` is generated in the `script_dir` and stores 
the transfer UUIDs of this transfer.

##### `replicate_sync_metadata`

The `TASKID_${project}_${node}:YYYY-MM_DD` will be sorted based on their timestamp. The code will process them from old to new depsite
the one generated today. Based on the transfer UUIDs for differnet batched globus transfers that are stored in the `TASKID` files, 
the skipped files (generaly cannot find the files in the soure endpoint) will be saved to a file. The transferred files are saved in the 
staging directory`ORNL_STAGE_DIR`. If all batched transfers are successful, rsync will the data to the production directory and 
change file and folder permission and follow the drs structures. 


#### Daily tasks

The daily tasks mainly include 

1. querying the ESGF1.5 index to find all records of data that was published yesterday 
at the monitoring site `QUEUE_queue_${project}_${node}:YYYY-MM_DD`.
2. from the above result, if the number of files are larger than zero, the function `get_file_list` will generate the
`QUEUE_transfer_${project}_${node}:YYYY-MM_DD` for globus to transfer data. If only datasets were published yesterday,
they will be replicated to ESGF1.5 index immediately. 

##### `get_file_list`

The function will use the `QUEUE_queue_${project}_${node}:YYYY-MM_DD` generated from `esgf15mms query-globus` with scroll search to get
the `id`,`type` and `url` of data published yesterday. It generates the transfer json file `QUEUE_transfer_${project}_${node}:YYYY-MM_DD`
to be used by `globus` to transfer the data from the data node where the data was published to the replicated data node.
