import os
from uuid import uuid4

from eth_utils import to_normalized_address, keccak
from requests import Session
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

engine = create_engine(
    os.environ.get('JSEARCH_MAIN_DB_TEST', "postgres://postgres:postgres@test_db/jsearch_main_test")
)
session: Session = scoped_session(sessionmaker(bind=engine, autocommit=True, autoflush=True))
Base = declarative_base()


def generate_address():
    return to_normalized_address(keccak(text=str(uuid4()))[-20:])
