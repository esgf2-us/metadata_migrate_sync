"""process the globus gmeta data class"""

from abc import ABC, abstractmethod
from typing import Any, Callable

class GmetaGenerator(ABC):
    """Abstract base class for GMeta list generation."""
    
    def generate(self, gdoc: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        """Template method that handles common generation logic."""
        gmeta_entries = []
        gmeta_entries_skipped: list[dict[str, Any]] = []
        
        for g in self.process_entry(gdoc["gmeta"]):
            gmeta_dict = {
                "id": g["entries"][0]["entry_id"],
                "subject": g["subject"],
                "visible_to": ["public"],
                "content": g["entries"][0]["content"],
            }
            
            if self.should_skip(g):
                gmeta_entries_skipped.append(gmeta_dict)
            else:
                gmeta_entries.append(gmeta_dict)
        
        return (
            {"ingest_type": "GMetaList", "ingest_data": {"gmeta": gmeta_entries}},
            {"ingest_type": "GMetaList", "ingest_data": {"gmeta": gmeta_entries_skipped}}
        )
    
    @abstractmethod
    def process_entry(self, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Process each entry before generation (can modify or filter)."""
        pass
    
    def should_skip(self, entry: dict[str, Any]) -> bool:
        """Determine if an entry should be skipped (can be overridden)."""
        return False


class StandardGmetaGenerator(GmetaGenerator):
    """Concrete implementation for standard GMeta generation."""
    
    def process_entry(self, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """No modification of entries."""
        return entries


class ModifiedGmetaGenerator(GmetaGenerator):
    """Concrete implementation for modified GMeta generation."""
    
    def __init__(self, 
        modifier: Callable[[dict[str, Any]], dict[str, Any]],
        **modifier_kwargs 
    ):
        self.modifier = modifier
        self.modifier_kwargs = modifier_kwargs
    
    def process_entry(self, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply modifier function to each entry."""
        return [self.modifier(entry, **self.modifier_kwargs) for entry in entries]


# Original function remains unchanged
def generate_gmeta_list_globus(gdoc: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Original function - generate gmeta list for ingestion from globus documents."""
    return StandardGmetaGenerator().generate(gdoc)


# New function with modifier
def generate_gmeta_list_globus_with_modifier(
    gdoc: dict[str, Any],
    modifier: Callable[[dict[str, Any]], dict[str, Any]]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Generate gmeta list with modification of each entry."""
    return ModifiedGmetaGenerator(modifier).generate(gdoc)
