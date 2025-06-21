import pathlib
import subprocess
from typing import Literal

import ijson
from globus_sdk import TransferClient

globus_endpoints = {
    "llnl": "1889ea03-25ad-4f9f-8110-1ce8833a9d7e",
    "anl": "8896f38e-68d1-4708-bce4-b1b3a3405809",
    "ornl": "7399956e-a57b-4560-b3d7-a035ff42cad4",
    "ornl-test": "7399956e-a57b-4560-b3d7-a035ff42cad4",
}




globus_path_prefix = {
    "llnl": "/",
    "anl": "/",
    "ornl": "/nl/themis/esgf/cli137/world-shared/globus/esg_dataroot",
    "ornl-test": "/nl/themis/esgf/cli137/mfx/Staged",
}

def paginate_json(file_path: str, page: int, per_page: int, json_type:str):
    """Paginate large JSON files using streaming."""
    start = (page - 1) * per_page
    end = start + per_page
    items = []

    if json_type == "RootDict":
        with open(file_path, 'rb') as f:
            timestamps = ijson.kvitems(f, '')
            for timestamp, data in timestamps:
                details = data.get('details', [])
                for i, item in enumerate(details):
                    if start <= i < end:
                        items.append(item["local_path"])
                    if i >= end:
                        break
    else: # RootArray
        with open(file_path, 'rb') as f:
            for i, item in enumerate(ijson.items(f, 'item')):
                if start <= i < end:
                    items.append(item["source_path"])
                if i >= end:
                    break
    return {
        'items': items,
        'current_page': page
    }

def _activate_ep(tc: TransferClient) -> None:

    for ep in globus_endpoints.values():
        r = tc.endpoint_autoactivate(ep)

        if r["code"] == "AutoActivationFailed":
            print(f"Autoactivation error of {ep}: r['message']")
        else:
            print(f"Successfully activate {ep}")


def _build_globus_transfer(
    source_ep: str,
    target_ep: str,
    file_name: str,
    label: str,
    options: str,
    batch_n: int,
) -> list[str]:

    return [
        'globus', 'transfer',
        f'{source_ep}',
        f'{target_ep}',
        '--batch',
        f'{file_name}',
        '--label',
        f'{label}-{batch_n}',
        f'{options}',
    ]


def _run(batch_n: int, cmd: list[str]) -> None:

    print("Transferring files...")
    try:
        # Using shell=True for input redirection
        result = subprocess.run(' '.join(cmd), shell=True, check=True)
        print(f"Batch {batch_n} submitted successfully")
    except subprocess.CalledProcessError as e:
        print(f"Error transferring batch {batch_n}: {e}")
    #-finally:
    #-    # Clean up batch file
    #-    os.remove('globus_batch.txt')

def globus_transfer(
    source_epname: Literal["llnl", "anl"],
    target_epname: Literal["ornl", "ornl-test"],
    path_list: list[str],
    batch_n: int,
    transfer_label: str = "cmip6",
) -> None:

    #-tc: TransferClient = GlobusClient.get_transfer_client()

    #-_activate_ep(tc)

    source_ep = globus_endpoints[source_epname]
    target_ep = globus_endpoints[target_epname]

    #-td = TransferData(tc,
    #-    source_ep,
    #-    target_ep,
    #-    encrypt_data = True,
    #-    verify_checksum = True,
    #-    preserve_timestamp = True,
    #-    fail_on_quota_errors = True,
    #-)

    src_path = pathlib.Path(globus_path_prefix[source_epname])
    dst_path = pathlib.Path(globus_path_prefix[target_epname])

    print (src_path, source_epname)
    print (dst_path, target_epname)


    batch_name = f"{transfer_label}_batch_{batch_n}.txt"

    with open(batch_name, 'w') as batch_file:
        for rpath in path_list:

            relative_path = rpath.removeprefix("/")
            src_file = src_path / relative_path
            dst_file = dst_path / relative_path

            #td.add_item(str(src_path), str(dst_path))
            batch_file.write(f"{src_file}  {dst_file}\n")

    cmd = _build_globus_transfer(
        source_ep,
        target_ep,
        batch_name,
        transfer_label,
        '--skip-source-errors -s exists',
        batch_n
    )

    print (cmd)
    _run(batch_n, cmd)

    #-try:
    #-    task = tc.submit_transfer(td)

    #-    print (task.get("status"))
    #-    print (task.get("task_id"))
    #-    print (task.get("request_time"))

    #-except Exception as e:

    #-    print("some error")
    #-    print(f"{source_epname}->{target_epname} - exception: {e}")
