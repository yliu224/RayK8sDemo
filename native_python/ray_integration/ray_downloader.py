#!/usr/bin/env python3
import argparse
import bz2
import json
import socket
import sys
import collections
import os
import ray
from dxpy.bindings.dxfile_functions import download_dxfile
from pathlib import Path
import dxpy
from dxpy.utils.resolver import resolve_existing_path

# Provide compatibility between python 2 and 3
USING_PYTHON2 = True if sys.version_info < (3, 0) else False
AUTH_INFO = {
        "auth_token_type": "Bearer",
        "auth_token": "***"
    }

def write_manifest_to_file(outfile, manifest):
    if USING_PYTHON2:
        with open(outfile, "w") as f:
            f.write(bz2.compress(json.dumps(manifest, indent=2, sort_keys=True)))
    else:
        # python 3 requires opening the file in binary mode
        with open(outfile, "wb") as f:
            value = json.dumps(manifest, indent=2, sort_keys=True)
            f.write(bz2.compress(value.encode()))
    return [outfile]

def fileID2manifest(fdetails, project):
    """
    Convert a single file ID to an entry in the manifest file
    Inputs: DNAnexus file and project ID
    Output: dictionary corresponding to manifest entry
    """
    if not fdetails:
        raise "Describe output for a file is None"
    pruned = {}
    pruned['id'] = fdetails['id']
    pruned['name'] = fdetails['name']
    pruned['folder'] = fdetails['folder']
     # Symlinks do not contain parts
    if fdetails['parts']:
        pruned['parts'] = {pid: {k:v for k,v in pdetails.items() if k == "md5" or k == "size"} for pid, pdetails in fdetails['parts'].items()}
    return pruned

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def write_split_manifest(manifest_file_name, manifest, num_files):
    manifest_files = []
    for project, file_list in manifest.items():
        for i, file_subset in enumerate(chunks(file_list, num_files)):
            manifest_subset = { project: file_subset }
            outfile = "{}_{:03d}.json.bz2".format(manifest_file_name.rstrip(".json.bz2"), i+1)
            with open(outfile, "wb") as f:
                f.write(bz2.compress(json.dumps(manifest_subset, indent=2, sort_keys=True).encode()))
            manifest_files.append(outfile)

    return manifest_files


def ensure_file_exists(file_path: str,file_name:str):
    if file_path.startswith("/"):
        file_path = file_path[1:]
    path = Path(f"/tmp/{file_path}/{file_name}")

    # Create parent directories if needed
    path.parent.mkdir(parents=True, exist_ok=True)

    # Create the file if it doesn't exist
    path.touch(exist_ok=True)
    return str(path)

@ray.remote
def download(manifest_name, data):
    dxpy.set_security_context(AUTH_INFO)

    project_name = next(iter(data))
    project = data[project_name]
    host_name = socket.gethostname()
    message = []
    for file in project:
        file_path = file["folder"]
        file_id = file["id"]
        file_name = file["name"]
        target_path = ensure_file_exists(file_path,file_name)
        download_dxfile(dxid=file_id,filename=target_path,project=project_name)
        message.append(f"Read data from {manifest_name} and write to {host_name}:{target_path}\n")
    return message

# $ python python_downloader.py "GxS_demo:data/GVCF" --recursive --output_file "test.manifest.json.bz2" --split 1 -d
def main():
    parser = argparse.ArgumentParser(description='Create a manifest file for a particular folder in a project')
    parser.add_argument('folder', help='a folder in the current DNAnexus project')
    parser.add_argument('-o', '--output_file', help='Name of the output file', default='manifest.json.bz2')
    parser.add_argument('-r', '--recursive', help='Recursively traverse folders and append to manifest', action='store_true', default=False)
    parser.add_argument('-s', '--split', help="Number of files per manifest", default=0, type=int)
    parser.add_argument('-d', '--download', help="Download files automatically", action='store_true', default=False)

    args = parser.parse_args()

    dxpy.set_security_context(AUTH_INFO)

    project, folder, _ = resolve_existing_path(args.folder)

    ids = dxpy.find_data_objects(classname='file', first_page_size=1000, state='closed', describe={'fields': {'id': True, 'name': True, 'folder': True, 'parts': True, 'state': True, 'archivalState': True }}, project=project, folder=folder, recurse=args.recursive)
    manifest = { project: [] }

    for i,f in enumerate(ids):
        manifest[project].append(fileID2manifest(f['describe'], project))
        if i%1000 == 0 and i != 0:
            print("Processed {} files".format(i))

    # Dedup
    # Duplicate filenames are converted to filename_fileid
    dups = [item for item, count in collections.Counter([x['name'] for x in manifest[project]]).items() if count > 1]
    for x in manifest[project]:
        if x['name'] in dups:
            fname, fext = os.path.splitext(x['name'])
            x['name'] = fname + "_" + x['id'] + fext

    manifest_files = []
    if args.split != 0:
        manifest_files = write_split_manifest(args.output_file, manifest, args.split)
    else:
        manifest_files = write_manifest_to_file(args.output_file, manifest)

    print("Manifest file written to {}".format(args.output_file))
    print("Total {} objects".format(len(manifest[project])))

    if args.download:
        ray.init()
        futures = []
        for mf in manifest_files:
            with bz2.open(mf, 'rt') as f:
                data = json.load(f)
                futures.append(download.remote(mf,data))

        results = ray.get(futures)
        print(results)



if __name__ == "__main__":
    main()