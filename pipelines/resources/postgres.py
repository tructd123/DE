"""
PostgreSQL Resource Configuration

Cấu hình kết nối PostgreSQL cho các pipelines.
"""

import os
from contextlib import contextmanager

import psycopg2
from dagster import ConfigurableResource, EnvVar


class PostgresResource(ConfigurableResource):
    """
    Resource để kết nối PostgreSQL.
    Sử dụng context manager để quản lý connection lifecycle.
    """
    host: str
    port: int
    database: str
    user: str
    password: str

    @contextmanager
    def get_connection(self):
        """Get a managed PostgreSQL connection."""
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
        )
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_connection_string(self) -> str:
        """Get SQLAlchemy connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


# Pre-configured resource for the zoomcamp database
postgres_zoomcamp = PostgresResource(
    host=EnvVar("POSTGRES_HOST"),
    port=EnvVar.int("POSTGRES_PORT"),
    database=EnvVar("POSTGRES_DB"),
    user=EnvVar("POSTGRES_USER"),
    password=EnvVar("POSTGRES_PASSWORD"),
)
