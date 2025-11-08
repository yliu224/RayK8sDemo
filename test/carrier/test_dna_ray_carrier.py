import os
import time
from test.carrier.test_dna_carrier import TestDNACarrier
from test.conftest import file_checker

import pytest

from demo.carrier.dna_ray_carrier import DNARayCarrier
from demo.path_mapper.hierarchy_mapper import HierarchyMapper

INPUT_FILE_PATH = "test_ray_carrier.txt"
TEXT = "hello ray"
CONTAINER_NAME1 = "raytest1"
CONTAINER_NAME2 = "raytest2"
OUTPUT_FOLDER = "test_output/ray"


@pytest.mark.parametrize("azurite_context", [[CONTAINER_NAME1, CONTAINER_NAME2]], indirect=True)
@pytest.mark.usefixtures("azurite_context")
@pytest.mark.parametrize("test_data_context", [(INPUT_FILE_PATH, TEXT, OUTPUT_FOLDER)], indirect=True)
@pytest.mark.usefixtures("test_data_context")
class TestDNARayCarrier(TestDNACarrier):
    def test_dna_ray_carrier(self, azurite: str) -> None:
        time.sleep(2)  # Wait for the container to be created
        source_fs, dest_fs = self.provide_source_and_dest_fs(CONTAINER_NAME1, CONTAINER_NAME2, azurite)
        path_mapper = HierarchyMapper()
        carrier = DNARayCarrier(source_fs, dest_fs, path_mapper)

        source_fs.upload_file(INPUT_FILE_PATH, "/test_data.txt")
        source_fs.upload_file(INPUT_FILE_PATH, "/test/test_data1.txt")
        source_fs.upload_file(INPUT_FILE_PATH, "/test_ray/ray_data.txt")

        carrier.move_folder("/", "/ray_test_output1/")
        actual_files = dest_fs.list_folder("/ray_test_output1")
        assert len(actual_files) == 3
        file_checker(
            actual_files,
            [
                {
                    "file_name": "test_data1.txt",
                    "file_size": 9,
                    "folder": "ray_test_output1/test",
                    "file_id": "xxxxx",
                    "file_path": "ray_test_output1/test/test_data1.txt",
                },
                {
                    "file_name": "test_data.txt",
                    "file_size": 9,
                    "folder": "ray_test_output1",
                    "file_id": "xxxxx",
                    "file_path": "ray_test_output1/test_data.txt",
                },
                {
                    "file_name": "ray_data.txt",
                    "file_size": 9,
                    "folder": "ray_test_output1/test_ray",
                    "file_id": "xxxxx",
                    "file_path": "ray_test_output1/test_ray/ray_data.txt",
                },
            ],
            expected_text=TEXT,
            fs=dest_fs,
            root_output_folder=os.path.join(OUTPUT_FOLDER, "1"),
        )

        carrier.move_folder("/test", "/ray_test_output2/")
        actual_files = dest_fs.list_folder("/ray_test_output2")
        assert len(actual_files) == 1
        file_checker(
            actual_files,
            [
                {
                    "file_name": "test_data1.txt",
                    "file_size": 9,
                    "folder": "ray_test_output2/test",
                    "file_id": "xxxxx",
                    "file_path": "ray_test_output2/test/test_data1.txt",
                },
            ],
            expected_text=TEXT,
            fs=dest_fs,
            root_output_folder=os.path.join(OUTPUT_FOLDER, "2"),
        )
