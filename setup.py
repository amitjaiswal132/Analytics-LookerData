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
    name = "Analytics-LookerData",
    version = "1.0",
    description = "Runs looker queries to warm up cache",
    author = "Amitj",
    packages = find_packages(),
    include_package_data=True,
    install_requires = [
        'pyyaml == 3.11',
        'requests == 2.9.1',
        'psycopg2 == 2.6.1',
        'Analytics-CommonUtilities >= 3.2.1',
        'Analytics-DailyUpdate >= 3.2.1',
        'python-dateutil >= 3.0'
    ],
    dependency_links = [
        "https://gdeploy:a13e8c8e9da9c0f06643146abd78ab1261934808@github.com/hike/Analytics-CommonUtilities/archive/master.zip#egg=Analytics-CommonUtilities-3.2.1"
    ],
    scripts=['daily_queries/scripts/looker_daily_query.py',
             ],
    data_files=datafiles,
    zip_safe=False
)
