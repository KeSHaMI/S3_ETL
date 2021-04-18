from abc import ABC, abstractmethod
import re
import pandas as pd
from sqlalchemy import (
    Integer, String, DateTime,
    Boolean, Date, Float,
    BigInteger
)


from datetime import datetime


class DataProcessor(ABC):

    @property
    @classmethod
    @abstractmethod
    def s3_type_name(cls) -> str:
        pass

    @property
    @classmethod
    @abstractmethod
    def db_table_name(cls) -> str:
        pass

    @property
    @classmethod
    @abstractmethod
    def db_data_types(cls) -> str:
        pass

    def __init__(self, data: pd.DataFrame):
        self.data = data

    @abstractmethod
    def process_data(self) -> pd.DataFrame:
        pass


class AppProcessor(DataProcessor):

    s3_type_name = 'app'
    db_table_name = 'apps'
    db_data_types = {
        'name': String,
        'genre': String,
        'rating': Float,
        'version': String,
        'size_bytes': BigInteger,
        'is_awesome': Boolean
    }

    def process_data(self):
        self.data['is_awesome'] = self.data['size_bytes'] % 256 == 0
        return self.data


class MovieProcessor(DataProcessor):
    s3_type_name = 'movie'
    db_table_name = 'movies'

    db_data_types = {
        'original_title': String,
        'original_language': String,
        'budget': Integer,
        'is_adult': Boolean,
        'release_date': Date,
        'original_title_normalized': String
    }

    def _get_normalized_title(self, title: str) -> str:
        """Removing all non-alphanumeric symbols and non-space from title and making each word lowercase"""
        return "_".join([re.sub("[^0-9a-zA-Z ]+", "", word).lower() for word in title.split()])

    def process_data(self):
        self.data['original_title_normalized'] = self.data.original_title.apply(self._get_normalized_title)
        self.data['release_date'] = pd.to_datetime(self.data['release_date'])
        return self.data


class SongProcessor(DataProcessor):
    s3_type_name = 'song'
    db_table_name = 'songs'

    db_data_types = {
        'artist_name': String,
        'title': String,
        'year': Integer,
        'release': String,
        'ingestion_time': DateTime,
    }

    def process_data(self):
        self.data['injection_time'] = datetime.now()
        return self.data

