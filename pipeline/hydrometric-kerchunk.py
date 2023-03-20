from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe, scan_file, finalize
from pangeo_forge_recipes.storage import FSSpecTarget, MetadataTarget, StorageConfig
from pangeo_forge_recipes.patterns import FilePattern, ConcatDim, MergeDim
from prefect import task, Flow, unmapped
import fsspec
from datetime import timedelta
import os
from datetime import datetime
import pandas as pd
from prefect.executors import LocalDaskExecutor
from kerchunk.combine import MultiZarrToZarr
import json
from kerchunk.zarr import single_zarr 
import fsspec.implementations.reference
import xarray as xr
import ujson
import dask

from config import Config


def make_filename(variable, time):
    return f"{Config.E5_HTTPS_BUCKET}/{time.strftime('%Y%m%d')}_{str.upper(variable)}_ERA5_SL_REANALYSIS.nc"


@task()
def get_files_to_process():

    """
    Determines list of all possible unique single variable daily files from a list of dates.
    It then compares if those files exist in the bucket (Config.BUCKET)

    :return: Matrix with dates and variables to extract
    """
    fs = fsspec.filesystem('s3', **Config.STORAGE_OPTIONS)
    current_filenames_in_bucket: list = ['s3://' + filename
                                         for filename in fs.glob(os.path.join(Config.HYDROMETRIC_BUCKET_ZARR_TS, '*'))]

    return current_filenames_in_bucket


@task()
def extract_metadata(current_filenames_in_bucket):
    """
    Determines list of all possible unique single variable daily files from a list of dates.
    It then compares if those files exist in the bucket (Config.BUCKET)

    :return: Matrix with dates and variables to extract
    """

    def f(path):
        return single_zarr(path, storage_options=Config.STORAGE_OPTIONS)

    tasks = [dask.delayed(f)(path)
         for path in current_filenames_in_bucket]
    
    refs = dask.compute(*tasks)
    return refs


@task()
def update_reference_file(refs):
    """
    Determines list of all possible unique single variable daily files from a list of dates.
    It then compares if those files exist in the bucket (Config.BUCKET)

    :return: Matrix with dates and variables to extract
    """
    # returns dicts from remote

    if len(refs) == 1:
        out = refs[0]
    else:
        mzz = MultiZarrToZarr(
            refs,
            remote_protocol='s3',
            remote_options=Config.STORAGE_OPTIONS,
            concat_dims=['id'],
        )

        # mzz does not support directly writing to remote yet
        # get dict version and push it
        out = mzz.translate()

    with open(Config.COMBINED_HYDROMETRIC_JSON, mode="wt") as f:
        f.write(json.dumps(out))

    fs = fsspec.filesystem('s3', **Config.STORAGE_OPTIONS)
    fs.put(Config.COMBINED_HYDROMETRIC_JSON,
           os.path.join(Config.HYDROMETRIC_ROOT_TS, Config.COMBINED_HYDROMETRIC_JSON),
           overwrite=True)


if __name__ == '__main__':

    with Flow("Hydrometric-kerchunk") as flow:
        current_filenames_in_bucket = get_files_to_process()
        refs = extract_metadata(current_filenames_in_bucket)
        update_reference_file(refs)
    flow.run()
