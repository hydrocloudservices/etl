from datetime import datetime, timedelta
import os


class Config(object):

    CLIENT_KWARGS = {'endpoint_url': 'https://s3.us-east-1.wasabisys.com',
                     'region_name': 'us-east-1'}
    CONFIG_KWARGS = {'max_pool_connections': 30}
    PROFILE = 'default'
    STORAGE_OPTIONS = {'profile': PROFILE,
                       'client_kwargs': CLIENT_KWARGS,
                       'config_kwargs': CONFIG_KWARGS
                       }

    ## Bucket configuration

    # ERA5 (single levels)
    E5_BUCKET = 's3://era5/world/reanalysis/single-levels/netcdf4'
    E5_HTTPS_BUCKET = 'https://s3.us-east-1.wasabisys.com/era5/world/reanalysis/single-levels/netcdf4'
    E5_REFERENCE_TARGET = 'era5/world/reanalysis/single-levels'
    E5_META_BUCKET = os.path.join(E5_REFERENCE_TARGET,'meta')

    E5_START_DATE = "2023-01-01"
    E5_VARIABLES = [
        "t2m",
        "tp",
    ]

    # ERA5-Land
    E5L_BUCKET = 's3://era5/north-america/reanalysis/land/netcdf4'
    E5L_HTTPS_BUCKET = 'https://s3.us-east-1.wasabisys.com/era5/north-america/reanalysis/land/netcdf4'
    E5L_REFERENCE_TARGET = 'era5/north-america/reanalysis/land'
    E5L_META_BUCKET = os.path.join(E5L_REFERENCE_TARGET,'meta')

    E5L_START_DATE = "2023-01-01"
    E5L_VARIABLES = [
        "t2m",
        "tp",
        "sd"
    ]

    # ERA5 (single levels - time series)
    E5_BUCKET_TS = 'https://s3.us-east-1.wasabisys.com/era5/world/reanalysis/single-levels/netcdf4'
    E5_BUCKET_ZARR_TS = 'era5/world/reanalysis/single-levels/zarr/timeseries_real_time'

    E5_BUCKET_ZARR_TS_NEW = 'era5/world/reanalysis/single-levels/zarr/timeseries'

    E5_TARGET_CHUNKS_TS = {"latitude": 7, "longitude": 7, "time": 1440}
    E5_START_DATE_TS = "1959-01-01"
    E5_START_DATE_TS_NEW = "1940-01-01"
    E5_VARIABLES_TS = [
        "t2m",
        "tp"
    ]

        # ERA5 (land - time series)
    E5L_BUCKET_TS = 'https://s3.us-east-1.wasabisys.com/era5/north-america/reanalysis/land/netcdf4'
    E5L_BUCKET_ZARR_TS = 'era5/north-america/reanalysis/land/zarr/timeseries_real_time'

    E5L_TARGET_CHUNKS_TS = {"latitude": 7, "longitude": 7, "time": 1440}
    E5L_START_DATE_TS = "1950-01-01"
    E5L_VARIABLES_TS = [
        "t2m",
        "tp",
        "sd"
    ]

        # USGS (time series)
    USGS_BUCKET_TS = 'https://s3.us-east-1.wasabisys.com/hydrometric/timeseries'
    USGS_BUCKET_ZARR_TS = 'hydrometric/timeseries'

        # Hydrometric (time series)
    COMBINED_HYDROMETRIC_JSON = 'streamflow_combined.json'
    HYDROMETRIC_BUCKET_TS = 'https://s3.us-east-1.wasabisys.com/hydrometric/timeseries'
    HYDROMETRIC_BUCKET_ZARR_TS = 'hydrometric/timeseries'
    HYDROMETRIC_ROOT_TS = 'hydrometric'
