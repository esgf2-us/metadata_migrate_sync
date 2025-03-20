from dataclasses import dataclass


@dataclass(frozen=True)
class SolrIndex:
    index_id: str
    index_name: str
    index_type: str | None = "solr"

class SolrIndexes:
    indexes: dict[str, SolrIndex]

    indexes = {
        "ornl": SolrIndex(
             index_name = "ornl", 
             index_id = "http://127.0.0.1:8983",
             index_type = "solr"
        ),
        "llnl": SolrIndex(
             index_name = "llnl", 
             index_id = "http://esgf-node.llnl.gov",
             index_type = "solr"
        ),
        "anl": SolrIndex(
             index_name = "anl", 
             index_id = "https://esgf-node.cels.anl.gov/",
             index_type = "solr"
        ),
    }
