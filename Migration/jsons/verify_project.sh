#!/usr/bin/env bash


array_arthmetric() {
    declare -n array1="$1"
    declare -n array2="$2"
    local optype="$3"

    result=()
    for ((i=0; i<${#array1[@]}; i++)); do
        if [[ ${#array2[@]} == 1 ]]; then
            if [[ $i == 1 ]]; then
                temp=${array2[0]//\"}
            else
                temp=0
            fi
        else
            temp=${array2[i]//\"}

        fi
        case "$optype" in
            '-') result+=($((${array1[i]//\"}-$temp))) ;;
            '+') result+=($((${array1[i]//\"}+$temp))) ;;
            *) 
                echo "Error: Invalid operation type '$optype'" >&2
                return 1
                ;;
        esac
    done
    echo "${result[@]}"
}

projects=(cmip3 cmip5 cmip6 create-ip e3sm-supplement geomip lucid tamip)
          #cmip6plus e3sm drcdp input4mips obs4mips)

/bin/rm -f chart_project.md
/bin/rm -f table_project.md
metas=(datasets files)

echo "\`\`\`mermaid
sankey-beta
 " > sankey_project_datasets.md

echo "\`\`\`mermaid
sankey-beta
 " > sankey_project_files.md

for prj in ${projects[@]}; do

    echo "!!! info \"$prj\" 
    <div style=\"display: flex; justify-content: center;\">" >> chart_project.md

    for meta in ${metas[@]}; do
        echo $prj

        if [[ $prj == "cmip6" || $prj == "cmip5" ]]; then
            numFound1=(`jq .[].numFound_last ${prj}_${meta}_summary.json`)
            recordSkipped1=(`jq '.[]."record skipped"' ${prj}_${meta}_summary.json`)
            recordTotal1=(`jq '.[]."record total"' ${prj}_${meta}_summary.json`)

            numFound2=(`jq .[].numFound_last gfdl-${prj}_${meta}_summary.json`)
            recordSkipped2=(`jq '.[]."record skipped"' gfdl-${prj}_${meta}_summary.json`)
            recordTotal2=(`jq '.[]."record total"' gfdl-${prj}_${meta}_summary.json`)

            IFS=$'\n' read -d '' -ra t < <(array_arthmetric numFound1 numFound2 '+')
            numFound=($t)

            IFS=$'\n' read -d '' -ra t < <(array_arthmetric recordSkipped1 recordSkipped2 '+')
            recordSkipped=($t)

            IFS=$'\n' read -d '' -ra t < <(array_arthmetric recordTotal1 recordTotal2 '+')
            recordTotal=($t)
        else
            numFound=(`jq .[].numFound_last ${prj}_${meta}_summary.json`)
            recordSkipped=(`jq '.[]."record skipped"' ${prj}_${meta}_summary.json`)
            recordTotal=(`jq '.[]."record total"' ${prj}_${meta}_summary.json`)
        fi
        
        IFS=$'\n' read -d '' -ra t < <(array_arthmetric recordTotal recordSkipped '-')
        recordIngested=($t)
        basename="${meta%?}"

        mprj=${prj^^}

        if [[ "$prj" == "e3sm-supplement" ]]; then
            mprj="e3sm-supplement"
        fi
        if [[ "$prj" == "geomip" ]]; then
            mprj="GeoMIP"
        fi
        
        echo $mprj

        echo "app_dev.py query-globus public ${mprj} --order-by _timestamp.asc --time-range TO2025-03-16 --printvar _timestamp --limit 1 --type=${basename^} | jq .total"
        globusTotal=`app_dev.py query-globus public ${mprj} --order-by _timestamp.asc --time-range TO2025-03-16 --printvar _timestamp --limit 1 --type=${basename^} | jq .total`
        
        imbal=$(( globusTotal - ${recordIngested[0]//\"} - ${recordIngested[1]//\"} - ${recordIngested[2]//\"} ))
        
        cat <<EOF >> chart_project.md
    \`\`\`mermaid
    pie showData
        title project: $prj-$meta
            "ANL" : $(echo ${recordIngested[0]} | bc )
            "LLNL" : $(echo ${recordIngested[1]} | bc )
            "ORNL" : $(echo ${recordIngested[2]} | bc )
            "IMBALANCE" :  $imbal
    \`\`\`
EOF

        cat <<EOF >> table_project.md
### $prj-$meta
| Index       | numFound           | Total records (skipped records) | Ingested (Entries) | 
| ----------- | ------------------ | ------------------------------- | ------------------ |
| ANL         | ${numFound[0]//\"} | ${recordTotal[0]//\"}(${recordSkipped[0]//\"}) | $(echo ${recordIngested[0]} | bc ) |
| LLNL        | ${numFound[1]//\"} | ${recordTotal[1]//\"}(${recordSkipped[1]//\"}) | $(echo ${recordIngested[1]} | bc ) |
| ORNL        | ${numFound[2]//\"} | ${recordTotal[2]//\"}(${recordSkipped[2]//\"}) | $(echo ${recordIngested[2]} | bc ) |
| PUBLIC      | | | $((${recordIngested[0]//\"} + ${recordIngested[1]//\"} + ${recordIngested[2]//\"}))($globusTotal)|



EOF
        cat <<EOF >> sankey_project_${meta}.md
ANL, ${prj},${recordTotal[0]//\"}
LLNL,${prj},${recordTotal[1]//\"}
ORNL,${prj},${recordTotal[2]//\"}

${prj},Skipped,${recordSkipped[0]//\"}
${prj},Skipped,${recordSkipped[1]//\"}
${prj},Skipped,${recordSkipped[2]//\"}

${prj},public,$globusTotal

EOF
    done
    echo "    </div>" >> chart_project.md
done

echo "\`\`\`" >> sankey_project_datasets.md

echo "\`\`\`" >> sankey_project_files.md
