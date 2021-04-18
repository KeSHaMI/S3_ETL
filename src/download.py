import json
import os
from typing import Type

import boto3
from botocore.client import BaseClient
import pandas as pd
from sqlalchemy.orm import sessionmaker

from src.data_processors import (
    DataProcessor, AppProcessor, MovieProcessor,
    SongProcessor
)

from src.db import ProcessedFile, engine
from src.logger import logger


class S3Bucket:
    bucket_name = os.getenv('BUCKET_NAME')

    def __init__(self):
        self._client: BaseClient = boto3.client('s3')

    def get_response_body(self, file_key: str) -> str:
        response = self._client.get_object(Bucket=self.bucket_name, Key=file_key)
        response_body: str = response['Body'].read().decode('utf-8')
        return response_body


class FileList:

    files_list_s3_name = os.getenv('FILE_LIST_NAME')

    def __init__(self, bucket: S3Bucket):
        self._bucket = bucket
        self._new_files = self._get_new_files()

    def _get_new_files(self):
        response_body = self._bucket.get_response_body(self.files_list_s3_name)
        filenames = response_body.split('\n')
        return filenames

    def __iter__(self) -> str:
        for file in self._new_files:
            yield file


class DataFile:

    PROCESSORS: list[Type[DataProcessor]] = [AppProcessor, MovieProcessor, SongProcessor]

    def __init__(self, filename: str, bucket: S3Bucket):
        self._filename = filename
        self._bucket = bucket
        self._engine = engine
        self.was_processed = self._check_if_was_processed()

    def _make_dataframe(self):
        file_data = self._bucket.get_response_body(self._filename)
        try:
            dataframe: pd.DataFrame = pd.read_json(file_data)
        except Exception:
            logger.error(f'ERROR {self._filename}')
            # pandas having trouble with opening file bce5476a-09e4-4e44-a3cc-eca0090a106c, and throws protocol error
            # but python json module deals with it nice
            file_data = json.loads(file_data)
            dataframe = pd.DataFrame(file_data)

        return dataframe

    def _check_if_was_processed(self) -> bool:
        Session = sessionmaker(bind=self._engine)
        session: Session = Session()
        obj = session.query(ProcessedFile).get(self._filename)
        session.close()
        return bool(obj)

    def load_data(self):
        dataframe = self._make_dataframe()
        for Processor in self.PROCESSORS:
            mask = dataframe['type'] == Processor.s3_type_name
            data = dataframe[mask]
            data = data['data']
            data_series = pd.DataFrame(data.to_list())
            processor = Processor(data_series)
            processed_data: pd.DataFrame = processor.process_data()

            processed_data.to_sql(
                processor.db_table_name, self._engine, if_exists='append',
                dtype=processor.db_data_types, method='multi'
            )
        self._add_to_processed_files()

    def _add_to_processed_files(self):
        Session = sessionmaker(bind=self._engine)
        session: Session = Session()
        file = ProcessedFile(filename=self._filename)
        session.add(file)
        session.commit()
        session.close()
