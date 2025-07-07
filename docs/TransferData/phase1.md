---
hide:
   - toc

---

| Project         | Missing at ORNL | Found at LLNL | Missing with errors             | Found at ANL | Remaining | Found in other way | Final missing | Transferred |
| --------------- | --------------- | ------------- | ------------------------------- | ------------ | --------- | ------------------ | ------------- | ----------- |
| CMIP6           | 515033          | 458444        | NotFound: 48854; PermDeny: 7735 | 39001        | 17588     | 7735               | 9853          | 505180      |
| CMIP5           | 39349           | 0             | NotFound: 39349                 |              |           | 38765              | 584           | 38765       |
| CMIP3           | 0               | 0             | 0                               |              |           |                    | 0             | 0           |
| e3sm-supplement | 3172            | 3172          | 0                               |              |           |                    | 0             | 3172        |
| GeoMIP          | 0               |               |                                 |              |           |                    | 0             | 0    |
| LUCID           | 0               |               |                                 |              |           |                    | 0             | 0    |
| TAMIP           | 0               |               |                                 |              |           |                    | 0             | 0    |
| CREATE-IP       | 0               |               |                                 |              |           |                    | 0             | 0    |
| CMIP6Plus       | 0               |               |                                 |              |           |                    | 0             | 0    |
| DRCDP           | 0               |               |                                 |              |           |                    | 0             | 0    |
| input4MIPs      | 6               | 6             | 0                               |              |           |                    | 0             | 6    |
| obs4MIPs        | 0               |               |                                 |              |           |                    | 0             | 0    |


There are two error codes from the globus transfer from LLNL to ORNL. 

  - FILE_NOT_FOUND ("NotFound") and 
  - PERMISSION_DENIED ("PermDeny"). 

The error of PERMISION_DENIED is caused by symbolic files. The error of FILE_NOT_FOUND is caused by no files on disk.


For CMIP5 missing files:

  - 38136 missing files under the path of `/gdo2_data/cmip5` that did not exist either on ANL and ORNL data nodes. Acutally, the directory is mapped to `/css03_data/cmip5/gdo2/cmip5` on LLNL server. So after changing the paths, all missing data were found at LLNL.

  - 1213 missing files under `cmip5_css02/data` that did not exist on ANL and ORNL data nodes, after changing to `cmip5_css02_data`, 629 files were found, but still 584 files cannot be found. 

