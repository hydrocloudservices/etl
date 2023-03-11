import intake
from pangeo_forge_recipes.recipes import XarrayZarrRecipe
from pangeo_forge_recipes.storage import FSSpecTarget, CacheFSSpecTarget
from pangeo_forge_recipes.storage import StorageConfig
from pangeo_forge_recipes.patterns import FilePattern, ConcatDim, MergeDim
import xarray as xr
import pandas as pd
import fsspec
from fsspec.implementations.local import LocalFileSystem
from pangeo_forge_recipes.executors.base import Pipeline, Stage
from pangeo_forge_recipes.recipes.xarray_zarr import store_chunk, cache_input
import datetime
import zarr
from prefect import task, Flow, unmapped
from pangeo_forge_recipes.recipes.xarray_zarr import prepare_target, finalize_target
import shutil
import os

from config import Config


def make_filename(variable, time):
    return f"{Config.E5L_BUCKET_TS}/{time.strftime('%Y%m%d')}_{str.upper(variable)}_ERA5_LAND_REANALYSIS.nc"


@task(nout=3)
def get_file_pattern():
    """
    Determines list of all possible unique single variable daily files from a list of dates.
    It then compares if those files exist in the bucket (Config.BUCKET)

    :return: Matrix with dates and variables to extract
    """
    fs = fsspec.filesystem('s3', **Config.STORAGE_OPTIONS)

    current_filenames_in_bucket: list = [os.path.basename(filename)
                                         for filename in fs.glob(os.path.join(Config.E5L_BUCKET.replace('s3://',''), '*.nc'))]
    end_date_2 = \
        max([datetime.datetime.strptime(filename.split('_')[0][0:8], '%Y%m%d').date()
             for filename in current_filenames_in_bucket]) \
        .strftime('%Y%m%d')

    delta_days = int(Config.E5L_TARGET_CHUNKS_TS['time']/24)

    # Get last index
    target_remote = FSSpecTarget(fs=fs, root_path=Config.E5L_BUCKET_ZARR_TS)
    try:
        store = target_remote.get_mapper()
        last_index = xr.open_zarr(store, consolidated=True).time[-1].dt.strftime('%Y-%m-%d').values.tolist()
        end_date = (datetime.datetime.strptime(last_index, '%Y-%m-%d') + datetime.timedelta(days=delta_days)).strftime('%Y%m%d')

        end_date = min([end_date, end_date_2])
    except:
        end_date = pd.date_range(Config.E5L_START_DATE_TS, periods=delta_days)[-1].strftime('%Y%m%d')
        pass

    years = pd.date_range(Config.E5L_START_DATE_TS, end_date)

    pattern = FilePattern(
        make_filename,
        ConcatDim(name="time", keys=years, nitems_per_file=24),
        MergeDim(name="variable", keys=Config.E5L_VARIABLES_TS)
    )
    return pattern, end_date, years


@task()
def create_recipe(pattern):
    """
    Determines list of all possible unique single variable daily files from a list of dates.
    It then compares if those files exist in the bucket (Config.BUCKET)

    :return: Matrix with dates and variables to extract
    """
    lfs = LocalFileSystem()
    target = FSSpecTarget(fs=lfs, root_path="timeseries_real_time")

    recipe = XarrayZarrRecipe(
        file_pattern=pattern,
        cache_inputs=False,
        target_chunks=Config.E5L_TARGET_CHUNKS_TS,
        storage_config=StorageConfig(target=target),
    )
    return recipe

# prepare_target_task1 = task(prepare_target, name='prepare_target')
@task()
def prepare_target_task(config):
    prepare_target(config=config)


@task()
def post_precess_dims(recipe, end_date):
    lfs = LocalFileSystem()
    target = FSSpecTarget(fs=lfs, root_path="timeseries_real_time")

    inclusive_end_date = (datetime.datetime.strptime(end_date, '%Y%m%d') + datetime.timedelta(days=1)).strftime(
        '%Y%m%d')
    store = recipe.storage_config.target.get_mapper()
    ds = xr.open_zarr(store, consolidated=False, decode_times=False)
    ds['time'] = pd.date_range(Config.E5L_START_DATE_TS, inclusive_end_date, freq='H', inclusive='left')
    ds.to_zarr(target.get_mapper(), compute=False, mode='a')
    zarr.consolidate_metadata(store)


@task()
def get_next_index(years):
    fs = fsspec.filesystem('s3', **Config.STORAGE_OPTIONS)
    target_remote = FSSpecTarget(fs=fs, root_path=Config.E5L_BUCKET_ZARR_TS)

    try:
        store = target_remote.get_mapper()
        last_index = xr.open_zarr(store, consolidated=True).time[-1].dt.strftime('%Y-%m-%d').values.tolist()
        df = (years.to_series().reset_index(name='date').date == last_index)
        next_index = (df[df == True].index.values[0] + 1) * len(Config.E5L_VARIABLES_TS)
    except:
        next_index = 0
        pass
    return next_index


@task()
def store_chunk_task(key, recipe):
    print(key)
    store_chunk(key, config=recipe)

@task()
def get_files_to_process(config, next_index):
    return list(config.iter_chunks())[next_index:]

@ task()
def finalize_target_task(recipe):
    finalize_target(config=recipe)

@ task()
def push_data_to_bucket():
    lfs = LocalFileSystem()
    target = FSSpecTarget(fs=lfs, root_path="timeseries_real_time")

    fs = fsspec.filesystem('s3', **Config.STORAGE_OPTIONS)
    target_remote = FSSpecTarget(fs=fs, root_path=Config.E5L_BUCKET_ZARR_TS)

    fs.put(target.root_path, os.path.dirname(Config.E5L_BUCKET_ZARR_TS), recursive=True)
    zarr.consolidate_metadata(target_remote.get_mapper())

    shutil.rmtree(target.root_path)


if __name__ == '__main__':

    with Flow("ERA5-Land-time-series") as flow:
        pattern, end_date, years = get_file_pattern()
        config = create_recipe(pattern)
        next_index = get_next_index(years, upstream_tasks=[config])
        prepare = prepare_target_task(config=config, upstream_tasks=[next_index])
        filenames = get_files_to_process(config, next_index, upstream_tasks=[prepare])
        store = store_chunk_task.map(filenames, recipe=unmapped(config))
        postprocess = post_precess_dims(config, end_date, upstream_tasks=[store])
        final = finalize_target_task(config, upstream_tasks=[postprocess])
        push_data_to_bucket(upstream_tasks=[final])

    flow.run()

