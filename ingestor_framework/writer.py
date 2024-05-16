from abc import ABC, abstractmethod
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming.query import StreamingQuery


@dataclass
class Writer(ABC):
    layer_config: dict

    @abstractmethod
    def write(self, df: DataFrame, dataset_config: dict) -> StreamingQuery:
        """Write data into layer

        :param DataFrame df: data to write
        :param dict dataset_config: dataset configuration
        :return StreamingQuery: streaming query object
        """
        pass


class BronzeWriter(Writer):
    def write(self, df: DataFrame, dataset_config: dict) -> StreamingQuery:
        """Write data into bronze layer

        :param DataFrame df: data to write
        :param dict dataset_config: dataset configuration
        :return StreamingQuery: streaming query object
        """
        datasource = dataset_config.get("datasource")
        dataset = dataset_config.get("dataset")
        opts = self.layer_config.get("options")
        opts.update(dataset_config.get("sink", {}).get("options", {}))
        base_path = self.layer_config.get("path")
        bronze_path = f"{base_path}/{datasource}/{dataset}/"
        checkpoint_path = f"{bronze_path}/_checkpoint/"
        return (
            df.withColumn("_ingested_at", F.current_timestamp())
            .writeStream.format(self.layer_config["format"])
            .partitionBy(dataset_config.get("sink", {}).get("partition_columns", []))
            .options(**opts)
            .option("checkpointLocation", checkpoint_path)
            .trigger(**(dataset_config.get("sink", {}).get("trigger", {"availableNow": True})))
            .queryName(f"{datasource} {dataset}")
            .start(bronze_path)
        )
