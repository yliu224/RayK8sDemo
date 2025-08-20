import logging
import os.path
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.types import Row, StringType, StructField, StructType

from demo.carrier.dna_carrier import DNACarrier
from demo.file_system.file_info import FileInfo

LOG = logging.getLogger(__name__)


class DNASparkCarrier(DNACarrier):
    SPARK_PARAM = StructType([StructField("FileInfo", StringType(), True)])

    def move_folder(self, source_folder: str, dest_folder: str, recursive: bool = True) -> None:
        spark = self.get_spark_session()

        def spark_download(row: Row) -> Row:
            tmp_folder = self.generate_tmp_folder()
            try:
                file_info = FileInfo.from_json(row.FileInfo)
                self.source.download_file(file_info, tmp_folder)
                self.dest.upload_file(os.path.join(tmp_folder, file_info.file_name), dest_folder)
                return Row(Message="True")
            except Exception as e:
                LOG.exception(e)
                return Row(Message=e)
            finally:
                shutil.rmtree(tmp_folder)
                LOG.info(f"Cleaned up {tmp_folder}")

        files = self.source.list_folder(source_folder, recursive)
        total = len(files)
        LOG.info(f"Found {total} files from {self.source_name}:{source_folder}")

        spark_params = [(f.to_json(),) for f in files]
        df = spark.createDataFrame(spark_params, self.SPARK_PARAM)
        LOG.info("Start download/upload files using spark")

        result = df.rdd.map(spark_download).toDF()
        failed_df = result.where("Message != 'True'")
        failed_count = failed_df.count()
        LOG.info(
            f"{total-failed_count}/{total} files successfully download from "
            f"{self.source_name}:{source_folder} to {self.dest_name}:{dest_folder}"
        )
        if failed_count != 0:
            failed_df.show(truncate=False)

    @staticmethod
    def get_spark_session() -> SparkSession:
        spark = SparkSession.builder.appName("MyApp").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark
