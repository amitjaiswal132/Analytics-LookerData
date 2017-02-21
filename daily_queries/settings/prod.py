from daily_queries.settings.common import *
NAGIOS_QUERY_FILE_FORMAT = "/var/log/nagios_check_files/looker_query_failed-%s"
_whitelisted_queries = { "117" : "",
                         "118" : "",
                         "140" : "",
                         "141" : ""}
SNIPER_END_POINT = "http://sniper.analytics.hike.in:4242/api/put?summary"
