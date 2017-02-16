import os

ENVIRONMENT = os.environ["ENVIRONMENT"] if os.environ.has_key("ENVIRONMENT") else "DEVELOPMENT"

print "environment", ENVIRONMENT
if ENVIRONMENT == "PRODUCTION":
    print "PRODUCTION"
    ENV_PREFIX = "Prod"
    from daily_queries.settings.prod import *
else:
    print "DEVELOPMENT"
    ENV_PREFIX = "Staging"
    from daily_queries.settings.dev import *
