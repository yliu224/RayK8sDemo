import logging
from typing import Any, Callable, List

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from demo.carrier.dna_carrier import DNACarrier
from demo.carrier.dna_spark_carrier_executor import DNASparkCarrierExecutor
from demo.file_system.file_info import FileInfo

LOG = logging.getLogger(__name__)


class DNASparkCarrier(DNACarrier):
    SPARK_PARAM = StructType([StructField("FileInfo", StringType(), True)])
    BATCH_SIZE = 10000  # TODO: Move this to config file maybe?
    NUM_OF_FILES_PER_PARTITION = 10  # TODO: Move this to config file maybe?

    def move_folder(self, source_folder: str, dest_folder: str, recursive: bool = True) -> None:
        self.sanity_check(self.source.get_file_system_name(), self.source.list_folder, source_folder, False)
        self.sanity_check(self.dest.get_file_system_name(), self.dest.list_folder, dest_folder, False)
        spark = self.get_spark_session()
        executor = DNASparkCarrierExecutor(self.source, self.dest, self.mapper, dest_folder)

        LOG.info(f"Starts moving files from {self.source_name}:{source_folder}")
        total = 0
        pre_index = 0
        failed_total = 0
        spark_params = []
        for idx, file in self.source.list_folder(source_folder, recursive):
            total = idx
            spark_params.append(file)
            if len(spark_params) >= self.BATCH_SIZE:
                LOG.info(f"Start download/upload files using spark from [{pre_index}-{total}]")
                failed_total += self.download_files(spark, spark_params, executor.spark_download)
                spark_params = []
                pre_index = total

        if spark_params:
            LOG.info(f"Start download/upload files using spark from [{pre_index}-{total}]")
            failed_total += self.download_files(spark, spark_params, executor.spark_download)
        LOG.info(
            f"{total-failed_total}/{total} files successfully download from "
            f"{self.source_name}:{source_folder} to {self.dest_name}:{dest_folder}"
        )

    def download_files(
        self, spark: SparkSession, spark_params: List[FileInfo], map_func: Callable[[FileInfo], str]
    ) -> int:
        files = spark.sparkContext.parallelize(
            spark_params, int(len(spark_params) / self.NUM_OF_FILES_PER_PARTITION) + 1
        )
        result = files.map(map_func).collect()
        failed_messages = list(filter(lambda x: x != "True", result))
        failed_count = len(failed_messages)
        if failed_count != 0:
            LOG.info(f"{failed_count} files failed in this batch!!!")
            for msg in failed_messages:
                LOG.error(msg)
        return failed_count

    @staticmethod
    def get_spark_session() -> SparkSession:
        spark = SparkSession.builder.appName("MyApp").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark

    @staticmethod
    def sanity_check(name: str, func: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        try:
            func(*args, **kwargs)
        except Exception as e:
            LOG.error(f"Failed to run sanity check for {name}: {e}")
            raise e
