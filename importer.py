import argparse
import datetime
import logging
import asyncio
import json
import uuid
import pandas as pd

from config.postgres_config import PostgresConfiguration
from src.postgres.connection_factory import ConnectionFactory
from src.postgres.data_repository import DataRepository
from datetime import date
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Data Importer")


async def import_data(args):
    # Get Database connection
    postgres_configuration = PostgresConfiguration()
    connection_factory = ConnectionFactory(postgres_configuration)
    data_repository = DataRepository(connection_factory, args.table_name, args.schema, args.stream, logger)

    instance_name = str(uuid.uuid4()).split("-")[0]

    metadata = {
      "day": date.today().day,
      "year": date.today().year,
      "month": date.today().month,
      "stream": args.stream,
      "tenant": args.schema,
      "product": args.table_name,
      "instance": f"{args.table_name}_{instance_name}"
    }

    logger.info(f"Running data importer for metadata {metadata}")
    await data_repository.build_database_objects()

    for folder, subfolders, files in os.walk(args.json_files_path):
        for file in files:
            if file.endswith(".json"):
                logging.info(f"Importing file {file} ... ")
                json_file_path = os.path.join(folder, file)
                with open(json_file_path, 'r') as json_file:
                    data = json.load(json_file)
                    json_strings = [json.dumps(item) for item in data]
                    airbyte_ab_id_strings = [str(uuid.uuid4()) for _ in data]
                    airbyte_emitted_at_timestamps = [datetime.datetime.now() for _ in data]
                    metadata_strings = [json.dumps(metadata) for _ in data]
                    df = pd.DataFrame({'_airbyte_ab_id': airbyte_ab_id_strings,
                                       '_airbyte_emitted_at': airbyte_emitted_at_timestamps,
                                       '_airbyte_data': json_strings,
                                       '_airbyte_metadata': metadata_strings})

                await data_repository.save_airbyte_raw_data(df)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Data Importer Service")
    parser.add_argument("--table_name", required=True, help="table name")
    parser.add_argument("--schema", required=True, help="schema")
    parser.add_argument("--stream", required=True, help="stream")
    parser.add_argument("--json_files_path", required=True, help="json_files_path")

    args = parser.parse_args()
    asyncio.run(import_data(args))