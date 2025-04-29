---
hide:
   - toc
title: Data node verification
---

<div id="tab_data_node"></div>

<script>
    document.addEventListener("DOMContentLoaded", function() {
      fetch('../jsons/data_node_verify.json')
        .then(response => response.json())
        .then(data => {
          // Initialize Tabulator with the fetched data
          var table = new Tabulator("#tab_data_node", {
            data: data, // Load data into the table
            layout: "fitColumn", // Fit columns to width of table
            pagination:"local",
            paginationSize:6,
            paginationSizeSelector:[3, 6, 8, 10, 50],
            columns: [ // Define table columns
              {formatter:"rownum", hozAlign:"center", width:40},
              { title: "Data Node", field: "data_node" },
              {
                  title: "Solr query",
                  columns:[
                    { title: "Datasets", field: "dataset_no",},
                    { title: "Files", field: "file_no"},
                    { title: "Total", field: "total_no"},
                  ],
              },
              {
                  title: "Solr project query",
                  columns:[
                    { title: "Datasets", field: "proj_dataset_no",},
                    { title: "Files", field: "proj_file_no"},
                    { title: "Total", field: "proj_total_no"},
                  ],
              },
              {
                  title: "Public query",
                  columns:[
                    { title: "Datasets", field: "public_dataset_no",},
                    { title: "Files", field: "public_file_no"},
                    { title: "Total", field: "public_no"},
                  ],
              },
            ],

            rowFormatter:function(row){
               var rowdata = row.getData();
               
               console.log(rowdata.proj_total_no == rowdata.public_no);
               if(rowdata.proj_total_no == rowdata.public_no){
                   row.getElement().style.color = "green";
               } else if(rowdata.proj_total_no > rowdata.public_no){
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

