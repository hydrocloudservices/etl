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

from config import Config


def make_filename(variable, time):
    return f"{Config.E5_HTTPS_BUCKET}/{time.strftime('%Y%m%d')}_{str.upper(variable)}_ERA5_SL_REANALYSIS.nc"


@task()
def get_file_pattern():

    """
    Determines list of all possible unique single variable daily files from a list of dates.
    It then compares if those files exist in the bucket (Config.BUCKET)

    :return: Matrix with dates and variables to extract
    """
    fs = fsspec.filesystem('s3', **Config.STORAGE_OPTIONS)
    current_filenames_in_bucket: list = [os.path.basename(filename)
                                         for filename in fs.glob(os.path.join(Config.E5_BUCKET, '*.nc'))]
    end_date = \
        max([datetime.strptime(filename.split('_')[0][0:8], '%Y%m%d').date()
             for filename in current_filenames_in_bucket]) \
        .strftime('%Y%m%d')

    dates = pd.date_range(Config.E5_START_DATE, end_date)

    pattern = FilePattern(
        make_filename,
        ConcatDim(name="time", keys=dates, nitems_per_file=24),
        MergeDim(name="variable", keys=Config.E5_VARIABLES)
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
    target = FSSpecTarget(fs, Config.E5_REFERENCE_TARGET)
    metadata = MetadataTarget(fs, Config.E5_META_BUCKET)
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
                                              for filename in fs.ls(Config.E5_META_BUCKET)]
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
def update_reference_file(config):
    """
    Determines list of all possible unique single variable daily files from a list of dates.
    It then compares if those files exist in the bucket (Config.BUCKET)

    :return: Matrix with dates and variables to extract
    """
    # finalize(recipe)
    assert config.storage_config.target is not None, "target is required"
    assert config.storage_config.metadata is not None, "metadata_cache is required"
    remote_protocol = fsspec.utils.get_protocol(next(config.file_pattern.items())[1])

    files = list(
        config.storage_config.metadata.getitems(
            list(config.storage_config.metadata.get_mapper())
        ).values()
    )  # returns dicts from remote
    if len(files) == 1:
        out = files[0]
    else:
        mzz = MultiZarrToZarr(
            files,
            remote_protocol=remote_protocol,
            remote_options=config.netcdf_storage_options,
            target_options=config.target_options,
            coo_dtypes=config.coo_dtypes,
            coo_map=config.coo_map,
            identical_dims=config.identical_dims,
            concat_dims=config.file_pattern.concat_dims,
            preprocess=config.preprocess,
            postprocess=config.postprocess,
        )
        # mzz does not support directly writing to remote yet
        # get dict version and push it
        out = mzz.translate()
    # fs = config.storage_config.target.fs
    with open(config.output_json_fname, mode="wt") as f:
        f.write(json.dumps(out))

    fs = fsspec.filesystem('s3', **Config.STORAGE_OPTIONS)
    fs.put(config.output_json_fname,
           os.path.join(config.storage_config.target.root_path, config.output_json_fname),
           overwrite=True)


if __name__ == '__main__':

    with Flow("ERA5-spatial-kerchunk") as flow:
        pattern = get_file_pattern()
        recipe = create_recipe(pattern)
        input_files = new_metadata_files(recipe)
        mapped_task=add_metadata_file.map(input_files, recipe=unmapped(recipe))
        update_reference_file(recipe, upstream_tasks=[mapped_task])
    flow.run()
