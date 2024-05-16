import json
from dataclasses import dataclass
from typing import Dict, List

from pyspark.sql import SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from ingestor_framework.reader import Reader, ReaderFactory
from ingestor_framework.writer import BronzeWriter, Writer


@dataclass
class IngestorFramework:
    config: dict
    readers: List[Reader]
    writers: Dict[str, Writer]

    @classmethod
    def build_from_config(cls, config: dict, spark: SparkSession = None):
        """ Build ingestion pipeline from a dict configuration

        :param dict config: pipeline configuration
        :param SparkSession spark: spark session
        :return _type_: _description_
        """
        if not spark:
            spark = SparkSession.builder.getOrCreate()
        readers = [
            ReaderFactory.get_reader(dataset_config.get("source"), config, spark)
            for dataset_config in config.get("datasets")
        ]
        layers = config.get("layers")
        writers = {"bronze": BronzeWriter(layers.get("bronze"))}
        return cls(config, readers, writers)

    @classmethod
    def build_from_file(cls, file_path, spark: SparkSession = None):
        """ Build ingestion pipeline from a json configuration

        :param dict config: pipeline configuration
        :param SparkSession spark: spark session
        :return _type_: _description_
        """
        with open(file_path) as file:
            config = json.load(file)

        return cls.build_from_config(config, spark)

    def run_pipeline(self):
        """Run ingestion pipeline
        """
        data = [reader.read() for reader in self.readers]
        sink_config_data_pairs = zip((dataset_config for dataset_config in self.config.get("datasets")), data)
        query_streams = [self.writers[config["sink"]["layer"]].write(data, config) for (config, data) in sink_config_data_pairs]

        for query in query_streams:
            try: 
              query.awaitTermination()
              print(f"🟢 ingestion '{query.name}' completed")
            except:
              print(f"🟠 ingestion '{query.name}' failed")
