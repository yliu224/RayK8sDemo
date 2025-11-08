import logging
import os
import shutil
from dataclasses import asdict
from subprocess import PIPE, Popen
from typing import Any, Dict, Iterator, List

import pytest
from azure.storage.blob import BlobServiceClient
from deepdiff import DeepDiff

from demo.file_system.file_info import FileInfo
from demo.file_system.file_system import FileSystem

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logging.getLogger("azure").setLevel(logging.WARNING)
LOG = logging.getLogger(__name__)

AZURITE_PORT = 10000
AZURITE_URL = "http://localhost:{port}"
AZURITE_ACCOUNT_NAME = "devstoreaccount1"
AZURITE_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="  # noqa: E501
AZURITE_ENDPOINT = f"{AZURITE_URL}/{AZURITE_ACCOUNT_NAME}"
AZURITE_CONNECTION_STRING = (
    f"DefaultEndpointsProtocol=http;AccountName={AZURITE_ACCOUNT_NAME};"
    f"AccountKey={AZURITE_KEY};BlobEndpoint={AZURITE_ENDPOINT};"
)


@pytest.fixture(scope="session")
def azurite() -> Iterator[str]:
    """Start Azurite and return the connection string."""
    # Start Azurite
    file_folder = "/tmp/azurite_test"
    cmd = ["azurite", "--location", file_folder, "--silent"]
    process = Popen(cmd, stdout=PIPE, stderr=PIPE)  # pylint: disable=R1732

    yield AZURITE_CONNECTION_STRING.format(port=AZURITE_PORT)

    process.terminate()
    try:
        stdout, stderr = process.communicate()  # This waits for the process
        LOG.info(stdout.decode())
        if process.returncode != 0:
            LOG.error(stderr.decode())
            raise RuntimeError(f"Failed to run command: {' '.join(cmd)}")
    except Exception as e:
        LOG.error(f"Error while running command: {' '.join(cmd)}")
        raise e
    finally:
        shutil.rmtree(file_folder, ignore_errors=True)


@pytest.fixture(scope="class")
def azurite_context(
    request: pytest.FixtureRequest,
    azurite: str,  # pylint: disable=W0621
) -> Iterator[None]:
    # before-class logic
    blob_client = BlobServiceClient.from_connection_string(azurite)
    containers = request.param
    for container in containers:
        blob_client.create_container(container)
    yield

    # after-class logic
    for container in containers:
        blob_client.delete_container(container)


@pytest.fixture(scope="class")
def test_data_context(
    request: pytest.FixtureRequest,
) -> Iterator[None]:
    """This function will create input and output folders and data for testing"""
    input_file_path, text, output_folder = request.param
    with open(input_file_path, "w", encoding="utf-8") as f:
        f.write(text)
    os.makedirs(output_folder, exist_ok=True)

    yield

    os.remove(input_file_path)
    shutil.rmtree(output_folder, ignore_errors=True)


def file_checker(
    actual_list: List[FileInfo],
    expected: List[Dict[str, Any]],
    expected_text: str,
    fs: FileSystem,
    root_output_folder: str,
) -> None:
    for idx, file_info in enumerate(actual_list):
        diff = DeepDiff(asdict(file_info), expected[idx], exclude_paths="file_id")
        if diff:
            LOG.error(diff)
        assert not diff
        output_folder = os.path.join(root_output_folder, file_info.folder)
        assert fs.download_file(file_info, output_folder) is True
        with open(os.path.join(output_folder, file_info.file_name), "r", encoding="utf-8") as f:
            assert f.read() == expected_text
