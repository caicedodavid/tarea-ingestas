from abc import ABC, abstractmethod
from dataclasses import dataclass

from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.streaming import DataStreamReader


@dataclass
class Reader(ABC):
    source_config: dict
    spark: SparkSession

    @abstractmethod
    def read(self) -> DataFrame:
        """Read data from source

        :return DataFrame: Read data
        """
        pass

    def set_config_options(self, reader: DataStreamReader) -> DataStreamReader:
        """Set all optional configuration for reading

        :param DataStreamReader reader: DataStreamReader object
        :return DataStreamReader: DataStreamReader object with options
        """
        return reader.options(**(self.source_config.get("options", {})))


class FileStorageReader(Reader):
    def read(self) -> DataFrame:
        """Read data from storage source

        :return DataFrame: Read data
        """
        schema_location = f"{self.source_config['path']}/_schema"
        reader = self.spark.readStream.format(self.source_config["format"]).option(
            "cloudFiles.schemaLocation", schema_location
        )
        reader = self.set_config_options(reader)
        return reader.load(self.source_config["path"]).withColumn("_filename", F.input_file_name())


@dataclass
class KafkaReader(Reader):
    kafka_config: dict
    client: SchemaRegistryClient = None

    def read(self) -> DataFrame:
        """Read data from Kafka source

        :return DataFrame: Read data
        """
        reader = self.spark.readStream.format(self.source_config["format"]).options(
            **(self.kafka_config["kafka_config"])
        )
        df = self.set_config_options(reader).load()
        key_formatter = self.get_parser_fuction(self.source_config["key_format"], "key")
        value_formatter = self.get_parser_fuction(self.source_config["value_format"], "value")
        return df.withColumn("key", key_formatter).withColumn("value", value_formatter)

    def get_parser_fuction(self, format: str, column_name: str) -> F.Column:
        """Get parser function depending on the format of the key or value of the kafka message

        :param str format: format of the key or value of kafka message
        :param str column_name: column in dataframe, should be one of key or value
        :raises ValueError: if format is not supported
        :return F.Column: parser function
        """
        if format == "string":
            return F.col(column_name).cast("string")
        if format == "json":
            return F.from_json(F.col(column_name).cast("string"), self.source_config["json_schema"])
        if format == "avro":
            avro_schema_string = self.get_avro_schema_string(column_name)
            return from_avro(F.expr("substring(value,6,length(value)-5)"), avro_schema_string)

        raise ValueError(f"The format {format} is not valid to parse the {column_name} of the Kafka message.")

    def get_avro_schema_string(self, column_name: str) -> str:
        """Get avro schema 

        :param str column_name: column in dataframe, should be one of key or value
        :return str: avro schema
        """
        client = self.get_schema_registry_client()
        topic = self.source_config["options"]["subscribe"]
        value_subject = f"{topic}-{column_name}"
        return client.get_latest_version(value_subject).schema.schema_str

    def get_schema_registry_client(self) -> SchemaRegistryClient:
        """Get schema registry client

        :return SchemaRegistryClient: Schema Registry client
        """
        if self.client:
            return self.client
        return SchemaRegistryClient(self.kafka_config["schema_registry_config"])


class ReaderFactory:
    FILE_STORAGE_FORMAT = "cloudFiles"
    KAFKA_FORMAT = "kafka"
    JSON_VALUE_FORMAT = "json"
    AVRO_VALUE_FORMAT = "avro"

    @classmethod
    def get_reader(cls, source_config: dict, app_config: dict, spark: SparkSession) -> Reader:
        """Factory method for building a reader

        :param dict source_config: configuration of source
        :param dict app_config: whole app configuration
        :param SparkSession spark: spark session
        :raises NotImplementedError: raised if no reader configured
        :return Reader: reader object
        """
        data_source = source_config["format"]
        if data_source == cls.FILE_STORAGE_FORMAT:
            return FileStorageReader(source_config, spark)
        if data_source == cls.KAFKA_FORMAT:
            kafka_config = {k: app_config[k] for k in app_config.keys() & {"kafka_config", "schema_registry_config"}}
            return KafkaReader(source_config, spark, kafka_config)
        raise NotImplementedError(f"The datasoure {data_source} is not implemented yet.")
