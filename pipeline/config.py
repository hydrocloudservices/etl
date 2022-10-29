from datetime import datetime, timedelta
import os

class Config(object):

    # Bucket configuration
    BUCKET = 's3://era5-atlantic-northeast/grib2/land/day'
    HTTPS_BUCKET = 'https://s3.us-east-1.wasabisys.com/era5/north-america/reanalysis/land/netcdf'
    REFERENCE_TARGET = 'era5/north-america/reanalysis/land'
    META_BUCKET = os.path.join(REFERENCE_TARGET,'meta')

    CLIENT_KWARGS = {'endpoint_url': 'https://s3.us-east-1.wasabisys.com',
                     'region_name': 'us-east-1'}
    CONFIG_KWARGS = {'max_pool_connections': 30}
    PROFILE = 'default'
    STORAGE_OPTIONS = {'profile': PROFILE,
                       'client_kwargs': CLIENT_KWARGS,
                       'config_kwargs': CONFIG_KWARGS
                       }
    # Dataset
    START_DATE = "1950-01-01"
    VARIABLES = [
        "t2m",
        "tp",
        "sd"
    ]
