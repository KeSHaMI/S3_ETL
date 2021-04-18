from datetime import datetime
from typing import Tuple

import pandas as pd

from src.download import S3Bucket, FileList, DataFile
from src.logger import logger
from src.db import engine


@logger.catch(level='ERROR')
def main() -> Tuple[int, int]:
    count_files = 0
    count_files_was_processed = 0
    bucket = S3Bucket()
    file_list = FileList(bucket)
    for file in file_list:
        data_file = DataFile(file, bucket)
        if data_file.was_processed:
            count_files_was_processed += 1
            logger.info(f'File: {file} was processed earlier')
        else:
            data_file.load_data()
            count_files += 1
            logger.info(f'File: {file} was processed')

    return count_files, count_files_was_processed


def log_data_examples():
    for processor in DataFile.PROCESSORS:
        data_iter = pd.read_sql_table(processor.db_table_name, engine, chunksize=5)
        for data in data_iter:
            logger.debug(data.to_csv())
            # For better file formatting
            logger.debug('\n\n')
            # reading only one chunk
            break


if __name__ == '__main__':

    print('Script started')
    start = datetime.now()
    count_files, count_files_was_processed = main()
    end = datetime.now()
    logger.info(f'{count_files} was processed')
    logger.info(f'{count_files_was_processed} was processed before, consider removing them from bucket')
    logger.info(f'Completion time: {end-start}')
    log_data_examples()
