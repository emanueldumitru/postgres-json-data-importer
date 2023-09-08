import os
from dotenv import load_dotenv


class PostgresConfiguration:
    def __init__(self):
        load_dotenv()

        # Postgres config
        self.postgres_olap_host = os.getenv("POSTGRES_OLAP_HOST")
        self.postgres_olap_port = os.getenv("POSTGRES_OLAP_PORT")
        self.postgres_olap_database = os.getenv("POSTGRES_OLAP_DATABASE")
        self.postgres_olap_username = os.getenv("POSTGRES_OLAP_USERNAME")
        self.postgres_olap_password = os.getenv("POSTGRES_OLAP_PASSWORD")
