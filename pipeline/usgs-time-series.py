from prefect import task, Flow, unmapped
import fsspec
from datetime import timedelta
import os
from datetime import datetime
import pandas as pd
from prefect.executors import LocalDaskExecutor
import json
import dataretrieval.nwis as nwis
import dask
import xarray as xr
from dask.distributed import Client
from timezonefinder import TimezoneFinder
import numpy as np
import xdatasets as xd
import dask_geopandas


from config import Config



def download_date(site):
    df = None
    try:
        print(site)
        df = nwis.get_record(sites=site, service='dv',  parameterCd='00060', start='1900-01-01', end='2023-03-06')
        df['00060_Mean'] = df['00060_Mean'].mask(df['00060_Mean'] < 0)
        df = df[['00060_Mean', '00060_Mean_cd']]
        df.columns = ['value', 'flag']
        df.index.names = ['time']
        
    except:
        pass
    return df


def process(df, site):
    ds = None
    try:
        ds = pd.DataFrame(index=pd.date_range('1900-01-01','2023-03-06')).to_xarray().rename({'index':'time'})
        ds_add = df.to_xarray()
        ds_add['time'] = pd.to_datetime(ds_add['time'])
        ds_add = ds_add.expand_dims({'id': [site]})
        ds_add = ds_add.assign_coords({'id': ('id', [site])})

        ds = xr.merge([ds, ds_add])
    except:
        pass
    return ds

def get_clim(gauge_id, latitude, longitude, poly, unique_id):
    data = None
    
    try:


        obj = TimezoneFinder()

        timezone = obj.timezone_at(lng=longitude, lat=latitude)
        timezone

        query = {
            "variables": {"era5_reanalysis_single_levels": ["t2m", "tp"]},
            "space": {
                "clip": "polygon", # bbox, point or polygon
                "aggregation": True,
                "geometry": poly,
                "unique_id": unique_id
            },
            "time": {
                "timestep": "D", # http://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
                "aggregation": {"tp": [np.nansum],
                                "t2m": [np.nanmax, np.nanmean, np.nanmin]},

                "start": '1978-12-31',
                "end": '2020-12-31',
                "timezone": timezone,
            },
        }
        data = xd.Dataset(**query).data
    except:
        pass
    return data

def merge_datasets(ds, ds_clim):
    ds_combined = None
    try:
        ds_clim = ds_clim.expand_dims('gauge_id').rename({'gauge_id': 'id'}).drop(['latitude','longitude','id'])
        ds_combined = xr.merge([ds, ds_clim])
    except:
        pass
    return ds_combined


def save_zarr(ds, site):
    try:
        if 'id' in ds.dims:
            ds.to_zarr(f's3://hydrometric/timeseries2/{site}', 
                        consolidated=True,
                        mode='w',
                        storage_options=Config.STORAGE_OPTIONS)
    except:
        pass


@task
def process_one_file(row):

    site, latitude, longitude, poly = row

    df = download_date(site)
    ds = process(df, site)
    ds_clim = get_clim(f"hysets_{site}", latitude, longitude, poly, "gauge_id")
    ds_merged = merge_datasets(ds, ds_clim)
    save_zarr(ds_merged, site)


if __name__ == '__main__':



    client = Client(processes=True)

    # Move hard-coded path to Config.py
    gdf = dask_geopandas.read_parquet('s3://hydrometric/shapefiles/geoparquet', 
                                      storage_options=Config.STORAGE_OPTIONS)

    # Will use a pilot DB instead
    df_stations = pd.read_csv('data/attributes_other_hysets.csv')
    df_stations['number'] = df_stations.apply(lambda row: row.gauge_id.split('_')[-1], axis=1)

    # Function to verify if file exists and if has been processed in the last 7 days
    # If not, then process then file
    fs = fsspec.filesystem('s3', **Config.STORAGE_OPTIONS)
    zarrs = fs.glob('hydrometric/timeseries/*')
    files_already_processed = [os.path.basename(file) for file in zarrs
                                   if fs.info(f'{file}/.zmetadata')['LastModified'].date() <
                                    (datetime.today() + timedelta(days=7)).date()
    ]

    iterable = []
    for idx, row in df_stations.iterrows():
        site = row.number
        latitude = row.gauge_lat
        longitude = row.gauge_lon
        if site not in files_already_processed:
            poly = gdf[gdf.gauge_id == f"hysets_{site}"].reset_index(drop=True).compute()
            iterable.append([site, latitude, longitude, poly])



    with Flow("USGS-time-series") as flow:
        process_one_file.map(iterable)


    from prefect.executors import DaskExecutor

    executor = DaskExecutor(address=client.scheduler.address)
    flow.run(executor=executor)

