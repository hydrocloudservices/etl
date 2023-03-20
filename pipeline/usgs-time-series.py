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
        df = nwis.get_record(sites=site, service='dv',  parameterCd='00060', start='1900-01-01', end=datetime.now().strftime('%Y-%m-%d'))
        df.iloc[:,0] = df.iloc[:,0].mask(df.iloc[:,0] < 0)
        df = df.iloc[:,:2]
        df.columns = ['discharge', 'discharge_flag']
        df.index.names = ['time']
        df['source'] = 'USGS'
        df = df.set_index(['source'], append=True)
        
    except:
        pass
    return df


def process(df, site):
    ds = None
    try:
        ds = pd.DataFrame(index=pd.date_range('1900-01-01',datetime.now().strftime('%Y-%m-%d'))).to_xarray().rename({'index':'time'})
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
            "datasets": {"era5_reanalysis_single_levels": {"variables": ["t2m", "tp"]},
                         "era5_land_reanalysis": {"variables": ["t2m", "tp", "sd"]}},
            "space": {
                "clip": "polygon", # bbox, point or polygon
                "averaging": True,
                "geometry": poly,
                "unique_id": unique_id
            },
            "time": {
                "timestep": "D", # http://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
                "aggregation": {"sd": [np.nanmean],
                                "tp": [np.nansum],
                                "t2m": [np.nanmax, np.nanmean, np.nanmin]},

                "start": '1978-12-31',
                "end": '2020-12-31',
                "timezone": timezone,
            },
        }
        data = xd.Query(**query).data
    except:
        pass
    return data

def merge_datasets(ds, ds_clim):
    ds_combined = None
    try:
        print(ds_clim)
        print(ds)
        ds_clim = ds_clim.drop(['latitude','longitude','id'])
        ds_combined = xr.merge([ds, ds_clim])
    except:
        pass
    return ds_combined


def save_zarr(ds, site):
    try:
        if 'id' in ds.coords:
            ds.to_zarr(f's3://hydrometric/timeseries/{site}', 
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
    ds_clim = get_clim(site, latitude, longitude, poly, "id")
    ds_merged = merge_datasets(ds, ds_clim)
    save_zarr(ds_merged, site)


if __name__ == '__main__':

    # Move hard-coded path to Config.py
    gdf = dask_geopandas.read_parquet('s3://hydrometric/watershed/combined_spatial.parquet', 
                                      storage_options=Config.STORAGE_OPTIONS)

    #gdf = gdf.loc[gdf.source.isin(['USGS'])]

    gdf = gdf.loc[gdf.source.isin(['USGS'])].compute()
    gdf = gdf.reset_index(drop=True)
    # # Will use a pilot DB instead
    # df_stations = pd.read_csv('data/attributes_other_hysets.csv')
    # df_stations['number'] = df_stations.apply(lambda row: row.gauge_id.split('_')[-1], axis=1)

    # Function to verify if file exists and if has been processed in the last 7 days
    # If not, then process then file
    fs = fsspec.filesystem('s3', **Config.STORAGE_OPTIONS)
    zarrs = fs.glob('hydrometric/timeseries/*')
    files_already_processed = [os.path.basename(file) for file in zarrs
                                   if fs.info(f'{file}/.zmetadata')['LastModified'].date() <
                                    (datetime.today() + timedelta(days=7)).date()
    ]

    iterable = []
    for idx, row in gdf.iterrows():
        try:
            latitude = None
            longitude = None
            site = row.id

            oneSite = nwis.what_sites(sites=site)
            latitude = oneSite[0].dec_lat_va.values[0]
            longitude = oneSite[0].dec_long_va.values[0]
            print(f"{idx}: {site}")
            if site not in files_already_processed:
                poly = gdf.loc[gdf.id == site]
                iterable.append([site, latitude, longitude, poly])
        except:
            pass

    print(len(iterable))
    with Flow("USGS-time-series") as flow:
        process_one_file.map(iterable[0:60])

    flow.run()

