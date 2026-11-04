'''Koneksi ke PostgreSQL di laptop. Host, user, password diambil dari file .env (PG_*).'''
import os

from sqlalchemy import create_engine


def get_engine():
    env = os.environ
    return create_engine(
        f"postgresql+psycopg2://{env['PG_USER']}:{env['PG_PASSWORD']}@{env['PG_HOST']}:{env['PG_PORT']}/{env['PG_DB']}"
    )
