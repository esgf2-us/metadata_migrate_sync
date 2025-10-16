"""Document conversion module."""
import datetime
import re
from typing import Any, Literal

from metadata_migrate_sync.esgf_index_schema.schema_solr import DatasetDocs, FileDocs
from metadata_migrate_sync.provenance import provenance


# Precompile regex patterns once (module-level constants)
_DOMAIN_PATTERN = re.compile(r'(?<=://)([^/:]+)(:\d+)?(/|$)')
_UUID_PATTERN = re.compile(r'(?<=globus:)[^/]+(?=/)')  # Matches UUID only

def convert_to_esgf_1_5(
    solr_doc: FileDocs | DatasetDocs | dict[str, Any],
    metatype: Literal["datasets", "files"]
) -> dict[Any, Any] | None:
    """Convert solr documents to the ESGF-1.5 documents."""
    esgf_doc = solr_doc

    # change.md #3a78b9f
    if "index_node" in esgf_doc:
        esgf_doc["index_node"] = "us-index"

    # remove the uri in datasets
    if "url" in esgf_doc and metatype == "datasets":
        _ = esgf_doc.pop('url', None)

    # filter out ornl copies
    if provenance._instance.source_index_name == "ornl" and "data_node" in esgf_doc:
        data_node = esgf_doc["data_node"]

        if ".llnl.gov" in data_node or ".anl.gov" in data_node or data_node == "esgf-node.ornl.gov":
            return esgf_doc
        else:
            return None

    # for e3sm
    if provenance._instance.source_index_name == "llnl" and "source_id" in esgf_doc:

        source_id = esgf_doc["source_id"][0]
        if source_id == "E3SM-2-1": # or "E3SM-2-1" in source_id
            return None

    return esgf_doc

def _process_urls(
    urls: list[str],
    data_node: str,
    globus_uuid: str,
) -> list[str]:

    new_uuid = globus_uuid

    new_urls = []
    new_urls_alt = []
    for url in urls:
        # Replace domain (keep port if exists)
        new_url = _DOMAIN_PATTERN.sub(lambda m: data_node + m.group(2) + '/' if m.group(2) else data_node + '/', url)
        # Replace UUID in globus URLs
        if new_url.startswith('globus:') and globus_uuid != "None":
            new_url = _UUID_PATTERN.sub(new_uuid, new_url)
            core_path = "/"+new_url.split("globus:")[1].split("|Globus|Globus")[0].split('/', 1)[1]
            urlg = new_url

        new_urls.append(new_url)

    if all('thredds' not in s for s in new_urls):

        url1 = f"https://esgf-node.ornl.gov/thredds/fileServer{core_path}|application/netcdf|HTTPServer"
        url2 = f"https://esgf-node.ornl.gov/thredds/dodsC{core_path}.html|application/opendap-html|OPENDAP"
        url3 = f"gsiftp://esgf-node.ornl.gov{core_path}|application/gridftp|GridFTP"
        new_urls_alt.extend([url1, url2, url3, urlg])

        return new_urls_alt
    else:
        return new_urls

def replicate_gmeta(
    gmeta: dict[str, Any],
    metatype: Literal["Dataset", "File"],
    source_data_node: Literal["llnl", "anl", "nersc"],
    target_data_node: Literal["ornl"],
    has_globus: bool=True,
    is_replica: bool=True,
) -> dict[Any, Any]:
    """
    Replicates GMETA metadata for ESGF data replication between nodes.

    Args:
        gmeta: Original metadata dictionary
        metatype: Type of metadata ("Dataset" or "File")
        source_data_node: Source data node identifier
        target_data_node: Target data node identifier

    Returns:
        Modified metadata dictionary for the target node
    """
    # Validate input parameters
    if metatype not in ("Dataset", "File"):
        raise ValueError(f"Invalid metatype: {metatype}")
    if source_data_node not in ("llnl", "anl", "nersc", "iap"):
        raise ValueError(f"Invalid source_data_node: {source_data_node}")
    if target_data_node not in ("ornl", "newiap"):
        raise ValueError(f"Unsupported target_data_node: {target_data_node}")

    # Define source and target mappings
    DN_MAPPINGS = {
        "llnl": ["esgf-data1.llnl.gov", "esgf-data2.llnl.gov", "aims3.llnl.gov"],
        "anl": ["eagle.alcf.anl.gov"],
        "nersc": ["esgf-data.nersc.gov"],
        "ornl": ["esgf-node.ornl.gov"],
        "iap": ["esg.lasg.ac.cn"],
        "newiap": ["esg.iap.ac.cn"]
    }

    GLOBUS_IDS = {
        "ornl": "dea29ae8-bb92-4c63-bdbc-260522c92fe8"
    }

    if source_data_node == "iap":  # now globus link in url
        has_globus=False
        is_replica=False

    src_dn_list = DN_MAPPINGS[source_data_node]
    tgt_dn_list = DN_MAPPINGS[target_data_node]

    tgt_globus_id = "None"
    if has_globus:
        tgt_globus_id = GLOBUS_IDS[target_data_node]

    if metatype == "File":
        try:
            # id, subject, data_node, url
            subject_parts = gmeta["subject"].split('|')
            gmeta["subject"] = f"{subject_parts[0]}|{tgt_dn_list[0]}"
            gmeta["entries"][0]["content"]["id"] = gmeta["subject"]
            gmeta["entries"][0]["content"]["data_node"] = tgt_dn_list[0]

            newurl = _process_urls(
                gmeta["entries"][0]["content"]["url"],
                tgt_dn_list[0],
                tgt_globus_id,
            )
            gmeta["entries"][0]["content"]["url"] = newurl

            if is_replica:
                gmeta["entries"][0]["content"]["replica"] = True

            #dataset_id
            if "dataset_id" in gmeta["entries"][0]["content"]:
                #mxu some dataset_id is list in some doc!!!
                temp_dataset_id = gmeta["entries"][0]["content"]["dataset_id"]

                if isinstance(temp_dataset_id, list):
                    dataset_parts = temp_dataset_id[0].split('|')
                else:
                    dataset_parts = temp_dataset_id.split('|')
                gmeta["entries"][0]["content"]["dataset_id"] = f"{dataset_parts[0]}|{tgt_dn_list[0]}"

            # update timestamp
            curtime = datetime.datetime.now(datetime.timezone.utc)
            timestamp = curtime.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
            gmeta["entries"][0]["content"]["_timestamp"] = timestamp

        except KeyError as e:
            raise ValueError(f"Missing required field in gmeta: {e}")
        return gmeta
    else:
        #subject id data_node, _timestampe replica=true
        try:
            # id, subject, data_node, replica
            subject_parts = gmeta["subject"].split('|')
            gmeta["subject"] = f"{subject_parts[0]}|{tgt_dn_list[0]}"
            gmeta["entries"][0]["content"]["id"] = gmeta["subject"]
            gmeta["entries"][0]["content"]["data_node"] = tgt_dn_list[0]

            if is_replica:
                gmeta["entries"][0]["content"]["replica"] = True

            # update timestamp
            curtime = datetime.datetime.now(datetime.timezone.utc)
            timestamp = curtime.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
            gmeta["entries"][0]["content"]["_timestamp"] = timestamp

            # edge case
            if "globus_url" in gmeta["entries"][0]["content"]:
                globus_url = gmeta["entries"][0]["content"]["globus_url"][0]
                globus_url_head=f"https://app.globus.org/file-manager?origin_id={tgt_globus_id}"
                new_globus_url=globus_url_head+"&"+globus_url.split("&")[1]
                gmeta["entries"][0]["content"]["globus_url"] = [new_globus_url]

        except KeyError as e:
            raise ValueError(f"Missing required field in gmeta: {e}")
        return gmeta


def _prepend_to_list_in_dict(
    content_dict: dict[str,Any],
    key: str,
    value: Any
) -> dict[str,Any]:
    """Helper function to prepend a value to a list in a dictionary."""
    if key in content_dict:
        content_dict[key] = [value, *content_dict[key]]
    else:
        content_dict[key] = [value]

def revise_gmeta(
    gmeta: dict[str, Any],
    revised_by: str,
    revised_items: dict[str, Any],
    revised_value: list[str],
    revised_option: Literal["exact", "include"] = "exact"
) -> dict[str, Any]:
    """Revise the Gmeta data from ESGF."""

    if len(revised_items.keys()) != len(revised_value):
        raise ValueError("Wrong revised items or values")

    if not gmeta.get("entries") or not gmeta["entries"][0].get("content"):
        raise ValueError("Invalid metadata structure - missing entries or content")

    for item, value in zip(list(revised_items.keys()), revised_value):
        if item in gmeta["entries"][0]["content"]:
            if revised_option == "exact":
                if revised_items[item] != gmeta["entries"][0]["content"][item]:
                    raise ValueError("the lists are not matched for revision")

                if isinstance(gmeta["entries"][0]["content"][item], list):
                    if (isinstance(value, list) and
                        len(value) == len(gmeta["entries"][0]["content"][item])):
                        gmeta["entries"][0]["content"][item] = value
                    else:
                        raise ValueError("wrong revised value")
                else:
                    gmeta["entries"][0]["content"][item] = value

            elif revised_option == "include":
                if isinstance(gmeta["entries"][0]["content"][item], list):
                    new_item = []
                    for strit in gmeta["entries"][0]["content"][item]:
                        if revised_items[item] not in strit:
                            raise ValueError("cannot find values in the metadata")
                        new_item.append(strit.replace(revised_items[item], value))
                    gmeta["entries"][0]["content"][item] = new_item
                else:
                    if revised_items[item] not in gmeta["entries"][0]["content"][item]:
                        raise ValueError("cannot find values in the metadata")
                    gmeta["entries"][0]["content"][item].replace(revised_items[item], value)

        else:
            print(f"No {item} in the doc of {gmeta['subject']}")


    # Get the content dictionary once to avoid repeated access
    content = gmeta["entries"][0]["content"]

    # Update revised_by
    _prepend_to_list_in_dict(content, "_revised_by", revised_by)

    # Update revised_item
    _prepend_to_list_in_dict(content, "_revised_item", '::'.join(revised_items.keys()))

    # Update timestamp
    curtime = datetime.datetime.now(datetime.timezone.utc)
    timestamp = curtime.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    _prepend_to_list_in_dict(content, "_revised_timestamp", timestamp)

    return gmeta


def _extract_version_from_id(text: str):
    # Look for patterns like vYYYYMMDD or YYYYMMDD
    pattern = r'v(\d{8})'
    match = re.search(pattern, text)
    if match:
        return match.group(1)
    return None

def _extract_scalar_value(value: Any) -> Any:
    """Extract scalar value from list if possible."""
    if isinstance(value, list):
        if len(value) == 1:
            return value[0]
        else:
            return None  # Multiple values, can't convert to scalar
    return value

def _convert_to_bool(value: Any) -> bool | None:
    """Convert various representations to boolean, returns None if conversion fails."""
    if isinstance(value, bool):
        return value
    
    if isinstance(value, list):
        if len(value) == 1:
            return _convert_to_bool(value[0])
        # For lists with multiple elements, conversion doesn't make sense
        return None
    
    if isinstance(value, str):
        value_lower = value.lower()
        if value_lower in ('true', '1', 'yes', 'y'):
            return True
        elif value_lower in ('false', '0', 'no', 'n'):
            return False
    
    # Try numeric conversion
    if isinstance(value, (int, float)):
        return bool(value)
    
    return None

def fix_dtype_gmeta(
    gmeta: dict[str, Any],
) -> dict[str, Any]:
    """data type fixes"""
    # 
    for item in ['latest', 'replica', 'retracted', 'deprecated']:
        if item not in gmeta["entries"][0]["content"]:
            continue
       
        value = gmeta["entries"][0]["content"][item]
        value_scalar = _extract_scalar_value(value)
        fixed_value = _convert_to_bool(value_scalar)
   
        if fixed_value is not None:
            gmeta["entries"][0]["content"][item] = fixed_value

    if 'version' in gmeta["entries"][0]["content"]:
        var_int = gmeta["entries"][0]["content"]["version"]
        fix_int = None

        metadata_id = gmeta["entries"][0]["content"]["id"]
        project_id = gmeta["entries"][0]["content"]["project"]
        # list -> scalar
        var_int_scalar = _extract_scalar_value(var_int)

        if var_int_scalar is not None:
            # check length 
            if len(str(var_int_scalar)) != 8:
                if _extract_version_from_id(metadata_id):
                    var_int_scalar = _extract_version_from_id(metadata_id)
            try:
                datetime.datetime.strptime(str(var_int_scalar), "%Y%m%d")
                fix_int = int(var_int_scalar)
            except ValueError:
                if str(var_int_scalar).isdigit() and (
                    "CMIP3" in project_id or "CMIP5" in project_id or
                    "e3sm-supplement" in project_id
                ):
                    fix_int = int(var_int_scalar)
                else:
                    fix_int = None
        else:
            fix_int = None

        if fix_int is not None:
            gmeta["entries"][0]["content"]["version"] = fix_int

        # for CMIP3, version predated it. so all set to 1
        if "CMIP3" in project_id:
            gmeta["entries"][0]["content"]["version"] = 1

    if 'dataset_id' in gmeta["entries"][0]["content"]:
        dataset_id = gmeta["entries"][0]["content"]["dataset_id"]
        dataset_id_fix = _extract_scalar_value(dataset_id)
        if dataset_id_fix is not None:
            gmeta["entries"][0]["content"]["dataset_id"] = dataset_id_fix


    # special fixes
    if "CMIP3" in project_id:
        if 'retracted' not in gmeta["entries"][0]["content"]:
            gmeta["entries"][0]["content"]["retracted"]=False

    if "input4MIPs" in project_id:
        if "25 km" in gmeta["entries"][0]["content"]["deprecated"]:
            if 'latest' in gmeta["entries"][0]["content"]:
                gmeta["entries"][0]["content"]["deprecated"] = not gmeta["entries"][0]["content"]["latest"]

    return gmeta
