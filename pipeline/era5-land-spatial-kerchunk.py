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

from config import Config


def make_filename(variable, time):
    return f"{Config.HTTPS_BUCKET}/{time.strftime('%Y%m%d')}_{str.upper(variable)}_ERA5_LAND_REANALYSIS.nc"


@task()
def get_file_pattern():

    """
    Determines list of all possible unique single variable daily files from a list of dates.
    It then compares if those files exist in the bucket (Config.BUCKET)

    :return: Matrix with dates and variables to extract
    """
    fs = fsspec.filesystem('s3', **Config.STORAGE_OPTIONS)
    current_filenames_in_bucket: list = [os.path.basename(filename)
                                         for filename in fs.ls(Config.BUCKET)]
    end_date = \
        max([datetime.strptime(filename.split('_')[0][0:8], '%Y%m%d').date()
             for filename in current_filenames_in_bucket]) \
        .strftime('%Y%m%d')

    dates = pd.date_range(Config.START_DATE, end_date)

    pattern = FilePattern(
        make_filename,
        ConcatDim(name="time", keys=dates, nitems_per_file=24),
        MergeDim(name="variable", keys=Config.VARIABLES)
    )
    return pattern


@task()
def create_recipe(pattern):
    """
    Determines list of all possible unique single variable daily files from a list of dates.
    It then compares if those files exist in the bucket (Config.BUCKET)

    :return: Matrix with dates and variables to extract
    """
    fs = fsspec.filesystem('s3', **Config.STORAGE_OPTIONS)
    target = FSSpecTarget(fs, Config.REFERENCE_TARGET)
    metadata = MetadataTarget(fs, Config.META_BUCKET)
    recipe = HDFReferenceRecipe(
        pattern,
        storage_config=StorageConfig(target=target, metadata=metadata),
        coo_map={"time": "cf:time"},
        output_storage_options={'anon': True},
        target_options={'anon': True},
        identical_dims=['latitude', 'longitude'],
    )
    return recipe


@task()
def new_metadata_files(recipe):
    """
    Determines list of all possible unique single variable daily files from a list of dates.
    It then compares if those files exist in the bucket (Config.BUCKET)

    :return: Matrix with dates and variables to extract
    """
    fs = fsspec.filesystem('s3', **Config.STORAGE_OPTIONS)

    all_inputs = list(recipe.iter_inputs())
    meta_files = [recipe.file_pattern[i].split('/')[-1] + '.json' for i in all_inputs]
    current_meta_filenames_in_bucket: list = [os.path.basename(filename)
                                              for filename in fs.ls(Config.META_BUCKET)]
    new_files = list(set(meta_files).difference(set(current_meta_filenames_in_bucket)))

    idx = [i for i, sublist in enumerate(meta_files)
           if sublist in new_files]

    all_new_inputs = [all_inputs[i] for i in idx]
    return all_new_inputs


@task(max_retries=5, retry_delay=timedelta(minutes=5))
def add_metadata_file(input_file, recipe):
    """
    Determines list of all possible unique single variable daily files from a list of dates.
    It then compares if those files exist in the bucket (Config.BUCKET)

    :return: Matrix with dates and variables to extract
    """
    scan_file(input_file, recipe)


@task()
def update_reference_file(recipe):
    """
    Determines list of all possible unique single variable daily files from a list of dates.
    It then compares if those files exist in the bucket (Config.BUCKET)

    :return: Matrix with dates and variables to extract
    """
    finalize(recipe)


if __name__ == '__main__':

    with Flow("ERA5-Land-spatial-kerchunk") as flow:
        pattern = get_file_pattern()
        recipe = create_recipe(pattern)
        input_files = new_metadata_files(recipe)
        mapped_task=add_metadata_file.map(input_files, recipe=unmapped(recipe))
        update_reference_file(recipe, upstream_tasks=[mapped_task])
    flow.run()
