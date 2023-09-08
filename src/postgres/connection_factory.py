import asyncpg

from config.postgres_config import PostgresConfiguration
from sqlalchemy import create_engine


class ConnectionFactory:

    def __init__(self, postgres_configuration: PostgresConfiguration):
        self.connection_info = postgres_configuration

    async def get_postgres_connection(self):
        connection = await asyncpg.connect(
            database=self.connection_info.postgres_olap_database,
            user=self.connection_info.postgres_olap_username,
            password=self.connection_info.postgres_olap_password,
            host=self.connection_info.postgres_olap_host,
            port=self.connection_info.postgres_olap_port,
            timeout=300
        )

        return connection

    def get_sql_alchemy_connection_engine(self):
        return create_engine(f"postgresql://{self.connection_info.postgres_olap_username}:"
                             f"{self.connection_info.postgres_olap_password}@"
                             f"{self.connection_info.postgres_olap_host}:"
                             f"{self.connection_info.postgres_olap_port}/{self.connection_info.postgres_olap_database}")