---
hide:
  - toc
title: Validation and Verification
---


!!! Warning

    The validation of the first migration showed that all the metadata at the LLNL, ANL and ORNL for the projects (except e3sm) in the design document 
    have been migrated to the public Globus index (the metadata of the read-write projects were also migrated to their corresponding staged indexes). 
    The discrepancies in the metadata counts (i.e., the total counts from the Solr indexes are less than the counts in the public indexes) 
    calculated on the basis of the data_nodes and institution_ids are due to the missing dataset and file metadata in the solr indexes. 
    We do not know the reason yet. 

## Sankey Diagram

Datasets:

{%
   include "Migration/jsons/sankey_project_datasets.md"
%}

Files:
{%
   include "Migration/jsons/sankey_project_files.md"
%}



<script>

    var k = 0;
    const observer = new MutationObserver(function(mutations, observer) {
        if (document.querySelectorAll('.node-labels').length > 0 ) {

            styleNodeLabels();
            k = k + 1;
            if (k>=2) {
                observer.disconnect();  // Stop observing after first run
            }
        }
    });
    observer.observe(document.body, {
        childList: true,
        subtree: true
    });

    function styleNodeLabels() {
        const nodeLabels = document.querySelectorAll('.node-labels');
        nodeLabels[0].style.fill = "#FF0099";

    };
</script>
