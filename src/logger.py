from loguru import logger


# loguru also allow to configure log rotation and many more useful features
logger.add('./logs/script_result.log', format="{time} {message}", level='INFO')
logger.add('./logs/errors.log', format="{time} {message}", level='ERROR')

