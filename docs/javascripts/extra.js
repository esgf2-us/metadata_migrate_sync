// Sample data
document.addEventListener("DOMContentLoaded", function() {

  if (!window.location.pathname.includes("Migration") && !window.location.pathname.includes("Synchronization")){
    return;
  }

  
  if (window.location.pathname.includes("Migration")){
    fetch('./jsons/index.json')
      .then(response => response.json())
      .then(data => {
        // Initialize Tabulator with the fetched data

        data.forEach(d => {
          d["used"] = parseFloat(d["size_in_mb"]) / parseFloat(d["max_size_in_mb"]) * 100.
        });
        
        var table = new Tabulator("#table1", {
          data: data, // Load data into the table
          layout: "fitDataTable", // Fit columns to width of table
          columns: [ // Define table columns
            { title: "Name", field: "display_name" },
            { title: "Index", field: "id", width: 300 },
            { title: "Size (MB)", field: "size_in_mb", width: 100 },
            { title: "Subjects", field: "num_subjects", width: 150 },
            { title: "Entries", field: "num_entries", width: 150 },
            { title: "Used (%)", field: "used", width: 150, hozAlign:"left", formatter:"progress",
              formatterParams:{
                  min:0,
                  max:100,
                  color:["green", "orange", "red"],
                  legend: function(value){return parseFloat(value).toFixed(1) + "%"},
                  legendColor:"inherit",
                  legendAlign:"center",
              }
            }
          ]
        });
      })
      .catch(error => console.error('Error loading JSON data:', error));

    var metas = ["datasets", "files"];
    var projs = ["cmip3", "cmip5", "cmip6", "create-ip", "e3sm-supplement", "geomip", "lucid", "tamip",
     "gfdl-cmip5", "gfdl-cmip6",
     "cmip6plus", "e3sm", "drcdp", "input4mips", "obs4mips"];
    var title = "Solr Index -> Public Index";
  } else {
    var metas = ["entries"];
    var projs = ["cmip6plus", "e3sm", "drcdp", "input4mips", "obs4mips"];
    var title = "Stage Indexes -> Public Index";
  }

  console.log(window.location.pathname.includes("Migration"))
   
  metas.forEach(meta => { 
    projs.forEach(proj => {
      load_json(title, proj, meta)
    });
  });

});


function load_json (title, proj, meta) {
      fetch("./jsons/" + proj + "_" + meta + "_summary.json")
        .then(response => response.json())
        .then(data => {
          var table = new Tabulator("#tab_" + proj + "_" + meta, {
            layout:"fitDataTable",
            columnDefaults:{
              resizable:false,
            },
            data: data,
            columns:[
                {title:title, field:"index_name", formatter:"textarea", vertAlign:"middle", sorter:"string"},
            ],

            rowFormatter:function(row){
                var element = row.getElement(),
                data = row.getData(),
                width = element.offsetWidth,
                rowTable, cellContents;

                //clear current row data
                while(element.firstChild) element.removeChild(element.firstChild);

                //define a table layout structure and set width of row
                rowTable = document.createElement("table")
                rowTable.style.width = (width - 18) + "px";
                rowTable.style.width = "800px";

                rowTabletr = document.createElement("tr");

                //add image on left of row
                cellContents = "<td text-align: middle;><div>" +  data.index_name + "</div></td>";

                if ((parseInt(data["page ingested"]) - parseInt(data["page skipped"]))  == parseInt(data["page checked"])) {
                    var ingest_success = "<span style='color:green;font-weight:bold;'>" + parseInt(data["page checked"]) + "</span>";
                }
                else {
                    var ingest_success = "<span style='color:red; font-weight:bold;'>" + parseInt(data["page checked"]) + "</span>";
                }

                if (parseInt(data["record total"]) == data.numFound_last) {
                    var total_record = "<span style='color:blue;font-weight:bold;'>" + parseInt(data["record total"]) + "</span>";

                }

                else {
                    var total_record = "<span style='color:brown;font-weight:bold;'>" + parseInt(data["record total"]) + "</span>";

                }

                if (data.numFound_first != data.numFound_last) {
                    var numfound_first = "<span style='color:red; font-weight:bold; text-decoration: underline;'>" + parseInt(data.numFound_first) + "</span>";
                }
                else
                {
                    var numfound_first = "<span style='color:darkmagenta; font-weight:bold;'>" + parseInt(data.numFound_first) + "</span>";
                }

                //add row data on right hand side
                cellContents += "<td>" + 
                              "<div><strong>Start & End time:</strong> " + data["start time"].slice(0, -7) + '--' + data["stop time"].slice(0,-7)  + 
                        "</div><div><strong>Query String:</strong> " + data["query string"] + 
                        "</div><div><strong>numFound at first:</strong> " + numfound_first + 
                        "</div><div><strong>numFound at last:</strong> " + data.numFound_last + 
                        "</div><div><strong>Page Queried:</strong> " + parseInt(data["page queried"]) + 
                        "</div><div><strong>Page Ingested:</strong> " + parseInt(data["page ingested"]) + " (skiped: " + parseInt(data["page skipped"]) + ")" +  
                        "</div><div><strong>Page Succeeded:</strong> " + ingest_success + 
                        "</div><div><strong>Record Skipped</strong> " + parseInt(data["record skipped"]) + 
                        "</div><div><strong>Record Total</strong> " + total_record + 
                        "</div><div><strong>Command line:</strong> " + data.cmd_line + 
                        "</div><div><strong>Hostname:</strong> " + data.hostname + 
                        "</div><div><strong>Error message:</strong> " + data.error + 
                        "</div><div><strong>Restart message:</strong> " + data.restart + 
                        "</div></td>"

                rowTabletr.innerHTML = cellContents;

                rowTable.appendChild(rowTabletr);

                //append newly formatted contents to the row
                element.append(rowTable);
            },

          });
        })  // data
        .catch(error => console.error('Error loading JSON data:', error));

};

