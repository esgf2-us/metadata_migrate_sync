
from metadata_migrate_sync.lite_model import enforced_field
from pydantic import ValidationError
import pytest



def test_lite_model():

    enforced_field(version=20250923, latest=True, replica=False, retracted=False)
    with pytest.raises(ValidationError):
         enforced_field(version=2025092, latest=True, replica=False, retracted=False)
    
    with pytest.raises(ValidationError):
         enforced_field(version=20251301, latest=True, replica=False, retracted=False)

