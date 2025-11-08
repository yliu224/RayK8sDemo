import logging
from typing import Any, Callable

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from demo.carrier.dna_carrier import DNACarrier
from demo.carrier.dna_spark_carrier_executor import DNASparkCarrierExecutor

LOG = logging.getLogger(__name__)


class DNASparkCarrier(DNACarrier):
    SPARK_PARAM = StructType([StructField("FileInfo", StringType(), True)])

    def move_folder(self, source_folder: str, dest_folder: str, recursive: bool = True) -> None:
        self.sanity_check(self.source.get_file_system_name(), self.source.list_folder, source_folder, False)
        self.sanity_check(self.dest.get_file_system_name(), self.dest.list_folder, dest_folder, False)

        spark = self.get_spark_session()
        executor = DNASparkCarrierExecutor(self.source, self.dest, self.mapper, dest_folder)

        files = self.source.list_folder(source_folder, recursive)
        total = len(files)
        LOG.info(f"Found {total} files from {self.source_name}:{source_folder}")

        spark_params = [(f.to_json(),) for f in files]
        df = spark.createDataFrame(spark_params, self.SPARK_PARAM)
        LOG.info("Start download/upload files using spark")

        result = df.rdd.map(executor.spark_download).toDF()
        failed_df = result.where("Message != 'True'")
        failed_count = failed_df.count()
        LOG.info(
            f"{total-failed_count}/{total} files successfully download from "
            f"{self.source_name}:{source_folder} to {self.dest_name}:{dest_folder}"
        )
        if failed_count != 0:
            failed_df.show(truncate=False)
        spark.stop()

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
