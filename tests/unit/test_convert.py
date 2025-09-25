from metadata_migrate_sync.convert import fix_dtype_gmeta
import pytest
import datetime


@pytest.mark.parametrize("gmeta_name,expected", [
    ("gmeta_sample_wrong_type", 1),
    ("gmeta_sample_right_type", 0),
])
def test_convert_fix_dtype(gmeta_name, expected, request):

     gmeta_sample = request.getfixturevalue(gmeta_name)
     
     #-gmeta_sample['gmeta'][0]["entries"][0]["content"]["latest"] = 'xxx'
     gmeta_sample['gmeta'][0]["entries"][0]["content"]["deprecated"] = ['TRUE']
     gmeta_sample['gmeta'][0]["entries"][0]["content"]["version"] = ['v1']
     gmeta_fixed = fix_dtype_gmeta(gmeta_sample['gmeta'][0])
     assert isinstance(gmeta_fixed["entries"][0]["content"]["latest"], int)
     assert isinstance(gmeta_fixed["entries"][0]["content"]["replica"], bool)
     assert isinstance(gmeta_fixed["entries"][0]["content"]["retracted"], bool)
     assert isinstance(gmeta_fixed["entries"][0]["content"]["deprecated"], bool)
     assert isinstance(gmeta_fixed["entries"][0]["content"]["version"], int)

     version = gmeta_fixed["entries"][0]["content"]["version"]

     print ('yyy', version, 'xxx')
     print ('yyy', gmeta_sample['gmeta'][0]["entries"][0]["content"]["deprecated"], 'xxx')
     assert datetime.datetime.strptime(str(version), "%Y%m%d") 

     if 'dataset_id' in gmeta_fixed["entries"][0]["content"]:
         assert isinstance(gmeta_fixed["entries"][0]["content"]["dataset_id"], str)



    
