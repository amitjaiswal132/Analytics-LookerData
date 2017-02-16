from logging import FileHandler
from daily_queries.settings import *
from analytics_common.utils.io_utils import IOUtils

class FileLoggingHandler(FileHandler):
    def __init__(self, mode="a"):
        IOUtils.create_file_if_not_exists(LOG_FILEPATH)
        FileHandler.__init__(self, LOG_FILEPATH, mode)
