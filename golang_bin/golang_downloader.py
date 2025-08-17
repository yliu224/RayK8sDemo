#!/usr/bin/env python3
import argparse
import bz2
import collections
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import dxpy
from dxpy.utils.resolver import resolve_existing_path

# Provide compatibility between python 2 and 3
USING_PYTHON2 = True if sys.version_info < (3, 0) else False


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
    pruned["id"] = fdetails["id"]
    pruned["name"] = fdetails["name"]
    pruned["folder"] = fdetails["folder"]
    # Symlinks do not contain parts
    if fdetails["parts"]:
        pruned["parts"] = {
            pid: {k: v for k, v in pdetails.items() if k == "md5" or k == "size"}
            for pid, pdetails in fdetails["parts"].items()
        }
    return pruned


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i : i + n]


def write_split_manifest(manifest_file_name, manifest, num_files):
    manifest_files = []
    for project, file_list in manifest.items():
        for i, file_subset in enumerate(chunks(file_list, num_files)):
            manifest_subset = {project: file_subset}
            outfile = "{}_{:03d}.json.bz2".format(
                manifest_file_name.rstrip(".json.bz2"), i + 1
            )
            with open(outfile, "wb") as f:
                f.write(
                    bz2.compress(
                        json.dumps(manifest_subset, indent=2, sort_keys=True).encode()
                    )
                )
            manifest_files.append(outfile)

    return manifest_files


def download(manifest):
    dir_path = os.path.dirname(os.path.abspath(__file__))
    result = subprocess.run(
        [f"{dir_path}/bin/dx-download-agent-macos", "download", manifest],
        capture_output=True,
        text=True,
    )
    return f"{manifest} return code is {result.returncode}"


# $ python golang_downloader.py "GxS_demo:data" --recursive --output_file "test.manifest.json.bz2" --split 1 -d
def main():
    parser = argparse.ArgumentParser(
        description="Create a manifest file for a particular folder in a project"
    )
    parser.add_argument("folder", help="a folder in the current DNAnexus project")
    parser.add_argument(
        "-o",
        "--output_file",
        help="Name of the output file",
        default="manifest.json.bz2",
    )
    parser.add_argument(
        "-r",
        "--recursive",
        help="Recursively traverse folders and append to manifest",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-s", "--split", help="Number of files per manifest", default=0, type=int
    )
    parser.add_argument(
        "-d",
        "--download",
        help="Download files automatically",
        action="store_true",
        default=False,
    )

    args = parser.parse_args()

    project, folder, _ = resolve_existing_path(args.folder)

    ids = dxpy.find_data_objects(
        classname="file",
        first_page_size=1000,
        state="closed",
        describe={
            "fields": {
                "id": True,
                "name": True,
                "folder": True,
                "parts": True,
                "state": True,
                "archivalState": True,
            }
        },
        project=project,
        folder=folder,
        recurse=args.recursive,
    )
    manifest = {project: []}

    for i, f in enumerate(ids):
        manifest[project].append(fileID2manifest(f["describe"], project))
        if i % 1000 == 0 and i != 0:
            print("Processed {} files".format(i))

    # Dedup
    # Duplicate filenames are converted to filename_fileid
    dups = [
        item
        for item, count in collections.Counter(
            [x["name"] for x in manifest[project]]
        ).items()
        if count > 1
    ]
    for x in manifest[project]:
        if x["name"] in dups:
            fname, fext = os.path.splitext(x["name"])
            x["name"] = fname + "_" + x["id"] + fext

    manifest_files = []
    if args.split > 0:
        manifest_files = write_split_manifest(args.output_file, manifest, args.split)
    else:
        manifest_files = write_manifest_to_file(args.output_file, manifest)

    print("Manifest file written to {}".format(args.output_file))
    print("Total {} objects".format(len(manifest[project])))

    if args.download:
        futures = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            for f in manifest_files:
                # Submit tasks to the thread pool
                futures.append(executor.submit(download, f))
                # Process results as they complete
        for future in as_completed(futures):
            print(future.result())


if __name__ == "__main__":
    main()
