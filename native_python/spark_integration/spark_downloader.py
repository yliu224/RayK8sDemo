#!/usr/bin/env python3
import argparse
import sys
import collections
import os
from dxpy.bindings.dxfile_functions import download_dxfile
from pathlib import Path
import dxpy
from dxpy.utils.resolver import resolve_existing_path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, Row

# Provide compatibility between python 2 and 3
USING_PYTHON2 = True if sys.version_info < (3, 0) else False
TOKEN = "***"
SPARK = SparkSession.builder.appName("CreateDFWithSchema").getOrCreate()


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

def to_df(data):
    project_name = next(iter(data))
    project = data[project_name]
    df_data = []
    for file in project:
        file_path = file["folder"]
        file_id = file["id"]
        file_name = file["name"]
        df_data.append((file_id,f"/tmp{file_path}/{file_name}",project_name,TOKEN))

    schema = StructType([
        StructField("fileId", StringType(), True),
        StructField("filePath", StringType(), True),
        StructField("project", StringType(), True),
        StructField("token", StringType(), True)
    ])

    return SPARK.createDataFrame(df_data, schema=schema)

def download(row:Row):
    path = Path(row.filePath)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Create the file if it doesn't exist
    path.touch(exist_ok=True)

    dxpy.set_security_context({
        "auth_token_type": "Bearer",
        "auth_token": row.token
    })

    download_dxfile(dxid=row.fileId, filename=row.filePath, project=row.project)
    return Row(message=f"{row.filePath} download success")

# $ python python_downloader.py "GxS_demo:data/GVCF" --recursive --output_file "test.manifest.json.bz2" --split 1 -d
def main():
    parser = argparse.ArgumentParser(description='Create a manifest file for a particular folder in a project')
    parser.add_argument('folder', help='a folder in the current DNAnexus project')
    parser.add_argument('-r', '--recursive', help='Recursively traverse folders and append to manifest', action='store_true', default=False)
    parser.add_argument('-d', '--download', help="Download files automatically", action='store_true', default=False)

    args = parser.parse_args()
    dxpy.set_security_context({
        "auth_token_type": "Bearer",
        "auth_token": TOKEN
    })

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

    print("Total {} objects".format(len(manifest[project])))

    manifest_df = to_df(manifest)

    if args.download:
        print(f"Start to download {manifest_df.count()} files")
        manifest_df.rdd.map(download).toDF().show(truncate=False)



if __name__ == "__main__":
    main()