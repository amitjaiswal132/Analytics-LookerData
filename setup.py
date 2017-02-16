# to install setuptools if isn't present
import ez_setup
import os
ez_setup.use_setuptools()

from setuptools import setup, find_packages

datafiles = [('.',['logging.ini'])]
for datadir in ["bin", "res"]:
    datafiles += [(root, [os.path.join(root, f) for f in files]) 
                  for root, dirs, files in os.walk(datadir)]
print datafiles

setup(
    name = "Analytics-DailyUpdate",
    version = "3.0",
    description = "Daily hive and redshift query execution",
    author = "Rishi",
    packages = find_packages(),
    include_package_data=True,
    install_requires = [
        'pyyaml == 3.11',
        'requests == 2.9.1',
        'psycopg2 == 2.6.1',
        'Analytics-CommonUtilities >= 3.2.1',
        'redis == 2.10.5',
        'redlock == 1.2.0',
        'python-dateutil == 2.6.0'
    ],
    dependency_links = [
        "https://gdeploy:a13e8c8e9da9c0f06643146abd78ab1261934808@github.com/hike/Analytics-CommonUtilities/archive/master.zip#egg=Analytics-CommonUtilities-3.2.1"
    ],
    scripts=['daily_update/scripts/redshift_daily_upload.py',
             'daily_update/scripts/redshift_backfill_days.py',
             'daily_update/scripts/redshift_backfill_history.py',
             'daily_update/scripts/redshift_check_x_percent_complete.py',
             'daily_update/scripts/redshift_update_hdr_parameters_1.py',
             'daily_update/scripts/schedule_hive_to_redshift.py'],
    data_files=datafiles,
    zip_safe=False
)
