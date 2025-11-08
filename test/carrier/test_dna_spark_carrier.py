import os
import time
from test.carrier.test_dna_carrier import TestDNACarrier
from test.conftest import file_checker

import pytest

from demo.carrier.dna_spark_carrier import DNASparkCarrier
from demo.path_mapper.hierarchy_mapper import HierarchyMapper

INPUT_FILE_PATH = "test_spark_carrier.txt"
TEXT = "hello spark"
CONTAINER_NAME1 = "sparktest1"
CONTAINER_NAME2 = "sparktest2"
OUTPUT_FOLDER = "test_output/spark"


@pytest.mark.parametrize("azurite_context", [[CONTAINER_NAME1, CONTAINER_NAME2]], indirect=True)
@pytest.mark.usefixtures("azurite_context")
@pytest.mark.parametrize("test_data_context", [(INPUT_FILE_PATH, TEXT, OUTPUT_FOLDER)], indirect=True)
@pytest.mark.usefixtures("test_data_context")
class TestDNASparkCarrier(TestDNACarrier):
    def test_dna_spark_carrier(self, azurite: str) -> None:
        time.sleep(2)  # Wait for the container to be created
        source_fs, dest_fs = self.provide_source_and_dest_fs(CONTAINER_NAME1, CONTAINER_NAME2, azurite)
        path_mapper = HierarchyMapper()
        carrier = DNASparkCarrier(source_fs, dest_fs, path_mapper)

        source_fs.upload_file(INPUT_FILE_PATH, "/test_data.txt")
        source_fs.upload_file(INPUT_FILE_PATH, "/test/test_data1.txt")
        source_fs.upload_file(INPUT_FILE_PATH, "/test/test_data2.txt")

        carrier.move_folder("/", "/spark_test_output1/")
        actual_files = dest_fs.list_folder("/spark_test_output1")
        assert len(actual_files) == 3
        file_checker(
            actual_files,
            [
                {
                    "file_name": "test_data1.txt",
                    "file_size": 11,
                    "folder": "spark_test_output1/test",
                    "file_id": "xxxxxx",
                    "file_path": "spark_test_output1/test/test_data1.txt",
                },
                {
                    "file_name": "test_data2.txt",
                    "file_size": 11,
                    "folder": "spark_test_output1/test",
                    "file_id": "xxxxxx",
                    "file_path": "spark_test_output1/test/test_data2.txt",
                },
                {
                    "file_name": "test_data.txt",
                    "file_size": 11,
                    "folder": "spark_test_output1",
                    "file_id": "xxxxxx",
                    "file_path": "spark_test_output1/test_data.txt",
                },
            ],
            expected_text=TEXT,
            fs=dest_fs,
            root_output_folder=os.path.join(OUTPUT_FOLDER, "1"),
        )

        carrier.move_folder("/test", "/spark_test_output2/")
        actual_files = dest_fs.list_folder("/spark_test_output2")
        assert len(actual_files) == 2
        file_checker(
            actual_files,
            [
                {
                    "file_name": "test_data1.txt",
                    "file_size": 11,
                    "folder": "spark_test_output2/test",
                    "file_id": "xxxxxx",
                    "file_path": "spark_test_output2/test/test_data1.txt",
                },
                {
                    "file_name": "test_data2.txt",
                    "file_size": 11,
                    "folder": "spark_test_output2/test",
                    "file_id": "xxxxxx",
                    "file_path": "spark_test_output2/test/test_data2.txt",
                },
            ],
            expected_text=TEXT,
            fs=dest_fs,
            root_output_folder=os.path.join(OUTPUT_FOLDER, "2"),
        )
