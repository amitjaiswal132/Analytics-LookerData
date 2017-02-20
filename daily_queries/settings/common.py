import pkg_resources
import os
from datetime import datetime
from daily_queries.settings import ENV_PREFIX

BASE_DIR = os.path.join(pkg_resources.resource_filename(__name__, ""), "../..")
LOG_CONFIG = os.path.join(BASE_DIR, "logging.ini")

WRITE_DIR = "/tmp/LookerDailyQueries/"
LOG_FILEPATH = os.path.join(WRITE_DIR, "logs", datetime.now().strftime("%Y-%M-%H-%M-%S.log"))

LOOKER_API_HOST = "https://hike.looker.com:19999"
LOOKER_API_LOGIN = LOOKER_API_HOST + "/login"
LOOKER_API_URL = LOOKER_API_HOST + "/api/3.0"
LOOKER_DASHBOARD_API_URL = LOOKER_API_URL + "/dashboards/%s"
LOOKER_LOOKUP_QUERY_API_URL = LOOKER_API_URL + "/looks/%s/run/sql?cache=true"
LOOKER_LOOKUP_RUN_API_URL = LOOKER_API_URL + "/looks/%s/run/json?cache=false"
LOOKER_CACHED_LOOKUP_RUN_API_URL = LOOKER_API_URL + "/looks/%s/run/json?cache=true"
LOOKER_METRIC_NAME = "looker_query_runtime"

LOOKER_DEFAULT_CLIENT_ID = "3Z2h9thmV5XSvpvdDSDt"
LOOKER_DEFAULT_CLIENT_SECRET = "8RwXbMWPgZmhYWf5tkwtxzty"

NAGIOS_QUERY_FILE_FORMAT = "/tmp/nagios_check_files/looker_query_failed-%s"

QUERY_TASKS_MAX_RETRY_ATTEMPT = 3

_whitelisted_queries = { "181" : "2342, 2325, 2337, 2338, 2324, 2339"}

def get_dashboard_queries():
    return _whitelisted_queries

def get_dashboard_look_id(dashboard):
    return _whitelisted_queries[dashboard]