import json
import os
import tempfile

import pandas as pd

from src.postgres.connection_factory import ConnectionFactory
from sqlalchemy import MetaData, Table, Column, String, DateTime, Text
from sqlalchemy.dialects.postgresql import JSONB
import pandas


class DataRepository:
    def __init__(self, connection_factory: ConnectionFactory, table_name, schema, stream, logger):
        self.connection_factory = connection_factory
        self.table_name = table_name
        self.stream = stream
        self.schema = schema
        self.logger = logger

    async def build_database_objects(self):
        """
        Create the state table if it does not exist.
        """
        connection = await self.connection_factory.get_postgres_connection()
        async with connection.transaction():
            self.logger.info("Creating schema if it doesn't exist ... ")
            await connection.execute(self._create_schema_query())

            self.logger.info("Creating raw table if it doesn't exist ... ")
            await connection.execute(self._create_airbyte_raw_data_table())
        await connection.close()

    def _create_schema_query(self) -> str:
        return f"""
                 CREATE SCHEMA IF NOT EXISTS \"{self.schema}\"
         """

    def _create_airbyte_raw_data_table(self):
        return f"""
               CREATE TABLE IF NOT EXISTS "{self.schema}"."_airbyte_raw_{self.table_name}_{self.stream}" (
                   _airbyte_ab_id character varying(255) COLLATE pg_catalog."default",
                   _airbyte_emitted_at timestamp without time zone,
                   _airbyte_data jsonb,
                   _airbyte_metadata jsonb
               )
        """

    async def save_airbyte_raw_data(self, chunk_dataframe: pandas.DataFrame):
        """
            Save a chunk to airbyte raw data

            :param chunk_dataframe:
        """
        connection = await self.connection_factory.get_postgres_connection()
        async with connection.transaction():
            with tempfile.NamedTemporaryFile(delete=False) as temp_csv_file:
                csv_file_path = temp_csv_file.name + ".csv"
                table_name = f"_airbyte_raw_{self.table_name}_" \
                             f"{self.stream}"

                chunk_dataframe.to_csv(csv_file_path, sep='\t', index=False, header=False)
                await connection.copy_to_table(table_name=table_name,
                                               schema_name=self.schema,
                                               delimiter='\t',
                                               source=csv_file_path, format='csv')
                os.remove(csv_file_path)
        await connection.close()