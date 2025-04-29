---
hide:
   - toc
title: Institution id verification
---

<div id="tab_institution_id"></div>

<script>
    document.addEventListener("DOMContentLoaded", function() {
      fetch('../jsons/institution_id_verify.json')
        .then(response => response.json())
        .then(data => {
          // Initialize Tabulator with the fetched data
          var table = new Tabulator("#tab_institution_id", {
            data: data, // Load data into the table
            layout: "fitColumn", // Fit columns to width of table
            pagination:"local",
            paginationSize:10,
            paginationSizeSelector:[3, 6, 8, 10, 50, 100],
            columns: [ // Define table columns
              {formatter:"rownum", hozAlign:"center", width:40},
              { title: "Institution", field: "institution_id" },
              {
                  title: "Dataset query",
                  columns:[
                    { title: "ORNL", field: "num_ornl_datasets", bottomCalc: "sum"},
                    { title: "ANL", field: "num_anl_datasets", bottomCalc: "sum"},
                    { title: "LLNL", field: "num_llnl_datasets", bottomCalc: "sum"},
                    { title: "Total Solr", field: "total_datasets_no"},
                    { title: "Public", field: "public_dataset_no"},
                  ],
              },
              {
                  title: "File query",
                  columns:[
                    { title: "ORNL", field: "num_ornl_files", bottomCalc: "sum"},
                    { title: "ANL", field: "num_anl_files", bottomCalc: "sum"},
                    { title: "LLNL", field: "num_llnl_files", bottomCalc: "sum"},
                    { title: "Total Solr", field: "total_files_no"},
                    { title: "Public", field: "public_file_no"},
                  ],
              },
            ],

            rowFormatter:function(row){
               var rowdata = row.getData();
               
               if(rowdata.total_datasets_no == rowdata.public_dataset_no || 
                  rowdata.total_files_no == rowdata.public_file_no)
               {
                   row.getElement().style.color = "green";
               } else if(rowdata.total_datasets_no < rowdata.public_dataset_no){
                   var cell1 = row.getCell("total_datasets_no");
                   var cell2 = row.getCell("public_dataset_no");
                   var cell3 = row.getCell("total_files_no");
                   var cell4 = row.getCell("public_file_no");
                   cell1.getElement().style.backgroundColor = "yellow";
                   cell2.getElement().style.backgroundColor = "yellow";
                   cell3.getElement().style.backgroundColor = "yellow";
                   cell4.getElement().style.backgroundColor = "yellow";
                   row.getElement().style.color = "red";
               }
               else {
                   row.getElement().style.color = "brown";
               }
            },
          });
        })
        .catch(error => console.error('Error loading JSON data:', error));
    })
</script>

