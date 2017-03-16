import time
import logging

import pytz
import requests
from analytics_common.commons.retry import retry
from analytics_common.threads.thread_with_stop_event import ThreadWithStopEvent
from daily_queries.settings import *
from daily_update.handlers.api_handler import ApiHandler
from analytics_common.commons.retry import retry
from analytics_common.utils.retry_utils import RetryUtils


class QueryHandler(ThreadWithStopEvent):

    logger = logging.getLogger(__name__)

    def __init__(self, dashboard, look_id, cached, token, date_str):

        ThreadWithStopEvent.__init__(self)
        self.look_id = look_id
        self.token = token
        self.complete = False
        self.date_str = date_str
        self.dashboard = dashboard
        self.cached = cached

    def run(self):
        job="Starting thread for look id %s" %(self.look_id)
        QueryHandler.logger.info(job)

        self.complete = False
        self._execute_query()
        self.complete = True

        job = "Finish thread look id %s" % (self.look_id)
        QueryHandler.logger.info(job)

    def is_complete(self):
        return self.complete

    def mark_completed(self):
        self.complete = True

    @staticmethod
    def push_metric_to_opentsdb(dashboard_id, look_id, instance, metric, value):
        try:
            bounded_instance = instance.split('-', 2)[-1]
            timestamp = int(time.time())
            QueryHandler.push_job_execution_metric_to_opentsdb(metric, value, timestamp, dashboard_id, look_id, bounded_instance)
            QueryHandler.logger.info('Execution push to opentsdb latency metric successful')
        except Exception as e:
            QueryHandler.logger.error('Failed to execute : push to opentsdb %s', str(e.message))

    @staticmethod
    @retry(always_retry=True, wait_fixed=2000, stop_func=RetryUtils.stop_after_n_attempts(3))
    def push_job_execution_metric_to_opentsdb(metric, value, timestamp, dashboard_id, look_id, job_instance):
        data = {}
        data["timestamp"] = timestamp
        data["metric"] = metric
        data["value"] = value
        data["tags"] = {
            "dashboardId": dashboard_id,
            "lookId": look_id,
            "jobInstance": job_instance
        }
        ApiHandler.logger.info("Job_execution: %s is to be posted to sniper", str(data))
        r = requests.post(SNIPER_END_POINT, json=data)
        r.raise_for_status()

    @retry(always_retry=True, is_instance_method=True, wait_fixed=5000, stop_func="_decide_stop_retry")
    def _execute_query(self):

        QueryHandler.logger.info("Starting look id %s \n",self.look_id)
        myheader = 'token %s' % (self.token)
        run_query_url = LOOKER_LOOKUP_RUN_API_URL % (self.look_id, str(self.cached).lower())
        query_start_time = datetime.now(pytz.utc)
        response = requests.get(run_query_url,
                                 headers={'Authorization': myheader})
        query_end_time = datetime.now(pytz.utc)
        QueryHandler.logger.info("############## %s %s ##############",self.look_id, self.date_str)
        QueryHandler.logger.info(response.content)
        value = (query_end_time - query_start_time).seconds
        looker_metric_name = LOOKER_METRIC_NAME % ("cached") if self.cached else LOOKER_METRIC_NAME % ("db")
        QueryHandler.push_metric_to_opentsdb(self.dashboard, self.look_id, self.date_str,
                                           looker_metric_name, value)
        QueryHandler.logger.info("##################################")

    def _decide_stop_retry(self, attempt_number, delay):
        '''
        If stop called or max_attempts exhausted return True
        :param attempt_number: current_attempt_number
        :param delay: time spent since method call
        '''
        if not self.stopped() and (attempt_number < QUERY_TASKS_MAX_RETRY_ATTEMPT):
            return False
        return True

    @staticmethod
    def wait_for_complete_and_remove( running_query_handlers):
        keep_waiting = True if len(running_query_handlers) > 0 else False
        while (keep_waiting):
            for query_handler in running_query_handlers:
                QueryHandler.logger.info("Waiting for %s - %s...",query_handler.look_id, query_handler.is_complete)
                if query_handler.is_complete():
                    running_query_handlers.remove(query_handler)
                    QueryHandler.logger.info("Complete Query: " + str(query_handler.look_id))
                    keep_waiting = False
                    # upload progress after every table_handler
            QueryHandler.logger.info("Sleeping ...\n")
            time.sleep(5)



