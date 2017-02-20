import time
import logging

import pytz
import requests
from analytics_common.commons.retry import retry
from analytics_common.threads.thread_with_stop_event import ThreadWithStopEvent
from daily_queries.settings import *
from daily_update.handlers.api_handler import ApiHandler

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
    def push_metric_to_opentsdb(job_type, name, is_hourly, instance, metric, value, status="success"):
        try:
            job_type, job_frequency, bounded_instance, timestamp = Utils.get_tags_of_job_execution(job_type, name,
                                                                                                   is_hourly, instance)
            ApiHandler.push_job_execution_metric_to_opentsdb(metric, value, timestamp, job_type, bounded_instance,
                                                             job_frequency, status)
            QueryHandler.logger.info('Execution push to opentsdb latency metric successful')
        except Exception as e:
            QueryHandler.logger.error('Failed to execute : push to opentsdb %s', str(e.message))

    @retry(always_retry=True, is_instance_method=True, wait_fixed=5000, stop_func="_decide_stop_retry")
    def _execute_query(self):

        QueryHandler.logger.info("Starting look id %s \n",self.look_id)

        myheader = 'token %s' % (self.token)
        run_query_url = LOOKER_LOOKUP_RUN_API_URL % (self.look_id, self.cached)
        response = requests.get(run_query_url,
                                 headers={'Authorization': myheader})

        QueryHandler.logger.info("############## %s %s ##############",self.look_id, self.date_str)
        query_start_time = datetime.now(pytz.utc)
        QueryHandler.logger.info(response.content)
        query_end_time = datetime.now(pytz.utc)
        name = "%s-%s" % (self.dashboard, self.look_id)

        looker_metric_name = LOOKER_METRIC_NAME % ("cached") if self.cached else LOOKER_METRIC_NAME % ("db")
        QueryHandler.push_metric_to_opentsdb("looker_query", name, True, self.date_str,
                                           looker_metric_name, (query_start_time - query_end_time).seconds)
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



