
#!/usr/bin/env bash

previous_status=$(esgf15mms query-globus backup input4MIPs --printvar id,retracted --id="input4MIPs.CMIP7.CMIP.UofMD.UofMD-landState-3-1-1.land.yr.multiple-management.gn.v20250325|esgf-data2.llnl.gov" |jq .retracted)

echo $previous_status

jq --argjson previous_status "$previous_status" \
'.revised_items.retracted = $previous_status
| .revised_value = [($previous_status | not)]' \
revise_conf.json > revise_conf.json.tmp \
&& mv revise_conf.json.tmp revise_conf.json

# change it
esgf15mms revise backup input4MIPs test_revise.json revise_conf.json Dataset
present_status=$(esgf15mms query-globus backup input4MIPs --printvar id,retracted --id="input4MIPs.CMIP7.CMIP.UofMD.UofMD-landState-3-1-1.land.yr.multiple-management.gn.v20250325|esgf-data2.llnl.gov" |jq .retracted)

echo $present_status
