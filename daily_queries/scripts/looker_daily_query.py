import sys
import os
import json
import requests

import datetime

from daily_queries.settings import *

from optparse import OptionParser
import logging.config
from analytics_common.utils.io_utils import IOUtils
from daily_queries.handlers.QueryHandler import QueryHandler
from analytics_common.utils.datetime_utils import DatetimeUtils

def parse_arguments():
    parser = OptionParser()
    parser.add_option("--client",
                      dest="client_id", default=LOOKER_DEFAULT_CLIENT_ID,
                      help="Provide the looker client  [default: %default]")
    parser.add_option("--secret",
                      dest="client_secret", default=LOOKER_DEFAULT_CLIENT_SECRET,
                      help="Provide the looker client secret [default: %default]")
    parser.add_option("--dashboards",
                      dest="dashboards", default=None,
                      help="comma separated dashboard ids [default: %default None means all]")
    parser.add_option("--look-ids",
                      dest="look_ids", default=None,
                      help="comma separated list of look ids [default: %default None means all]")
    parser.add_option("--parallel-runs",
                      dest="parallel_runs", default=10, type="int",
                      help="max number of queries to run in parallel [default: %default]")
    parser.add_option("-e", "--email",
                      dest="email", default=None,
                      help="send start and complete mail to these email ids (comma separated)  [default: %default]")
    parser.add_option("-n", "--nagios-hour-threshold",
                      dest="nagios_hour_threshold", default=12, type="int",
                      help="queries scheduled after this hour (ist) will not trigger nagios [default: %default]")

    (options, args) = parser.parse_args()
    return parser, options, args

def _get_look_id_list(access_token, dashboards=None, look_ids=None):
    dashboard_to_process={}

    if dashboards is not None:
        for dashboard in dashboards.split(","):
            if look_ids is not None:
                dashboard_to_process[dashboard]=look_ids
            else:
                dashboard_to_process[dashboard] = ""
    else:
        dashboard_to_process = get_dashboard_queries()

    myheader = "token %s" % (access_token)

    # Generate look_id list.
    # look id being passed from command have to be white listed
    look_run_id = []
    for dashboard in dashboard_to_process:
        #Verify all look_id from the dashboard api call of looker
        dashboard_url = LOOKER_DASHBOARD_API_URL % (dashboard)
        response = requests.get(dashboard_url, headers={'Authorization': myheader})
        dashboard_res = json.loads(response.content)

        for keys in dashboard_res['dashboard_elements']:
            if len(dashboard_to_process[dashboard])==0 or str(keys['look_id']) in dashboard_to_process[dashboard]:
                look_run_id.append(keys['look_id'])

        logger.info( "list of look_id for be warmed up : %s",look_run_id)
    return look_run_id


def main():
    logger.debug("sys.argv: %s", sys.argv)
    parser, options, args = parse_arguments()
    parallel_runs = options.parallel_runs
    nagios = False if DatetimeUtils.cur_ist_time().hour > options else True
    dashboards = options.dashboards if options.dashboards is not None else get_dashboard_queries()
    date_str = datetime.strptime(DatetimeUtils.cur_utc_time(), "%Y-%m-%d-%H")
    #Get Auth token
    cmd = '''curl -d  "client_id=%s&client_secret=%s"  %s''' % (options.client_id, options.client_secret, LOOKER_API_LOGIN)
    resposne = os.popen(cmd).read()
    #TODO Add code to verify the token status
    auth = json.loads(resposne)
    access_token = auth['access_token']

    myheader = 'token %s' % (access_token)
    running_query_handler = []

    error_msgs = ""

    for look_id in _get_look_id_list(access_token,options.dashboards, options.look_ids):
        if len(running_query_handler) >= parallel_runs:
            QueryHandler.wait_for_complete_and_remove(running_query_handler)
        try:
            run_query = QueryHandler(look_id, access_token, date_str)
            run_query.start()
            running_query_handler.append(run_query)
        except Exception, e:
            logger.exception("Error executing: %s", run_query)
            error_msgs += "\n Query: " + str(run_query) + "\nException: " + str(e) + "\n"

    if len(error_msgs) >= 0 and nagios:
        day_str = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        file_path = NAGIOS_QUERY_FILE_FORMAT % (day_str)
        logger.info(error_msgs)
        #IOUtils.write(file_path, error_msgs)

if __name__ == "__main__":
    IOUtils.create_file(LOG_FILEPATH)
    logging.config.fileConfig(LOG_CONFIG, disable_existing_loggers=False)
    logger = logging.getLogger(__name__)
    logger.info("LOG_FILEPATH: %s", LOG_FILEPATH)
    main()