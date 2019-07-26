import os

import datetime
from eth_utils import to_normalized_address, keccak
from requests import Session
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from uuid import uuid4

main_db_dsn = os.environ['JSEARCH_MAIN_DB']
main_db_engine = create_engine(main_db_dsn)

session: Session = scoped_session(sessionmaker(bind=main_db_engine, autocommit=True, autoflush=True))

Base = declarative_base()


def generate_address():
    return to_normalized_address(keccak(text=str(uuid4()))[-20:])


def generate_psql_timestamp():
    return datetime.time().strftime('%Y-%m-%d %H:%M:%S')
