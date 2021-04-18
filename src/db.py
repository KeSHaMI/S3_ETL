import os
from sqlalchemy import create_engine
from sqlalchemy import Column, String

import time

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import OperationalError

Base = declarative_base()


class ProcessedFile(Base):

    __tablename__ = 'processed_files'

    filename = Column(String, primary_key=True)


def init_db():
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD')
    db_name = os.getenv('POSTGRES_DB')

    try:
        engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@db:5432/{db_name}")

        # Deleting db data on each launch, to check previous data comment out this line
        # Base.metadata.drop_all(bind=engine)

        Base.metadata.create_all(bind=engine)
    except OperationalError:
        # This happens occasionally when db container launching postgres too long
        print('App started earlier than db')
        print('Retrying in 3 sec')
        time.sleep(3)
        return init_db()

    return engine


engine = init_db()
