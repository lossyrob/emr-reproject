#!/usr/bin/env python

"""Reprojection Spark job for GeoTiffs

This file contains a script that can be used via `spark-submit` to handle
reprojecting GeoTif files using Spark and rasterio.

Required parameters include a source directory for unprojected GeoTifs (either local or remote) and
a destination directory for a Hadoop Sequence File

Optional parameters include
 - destination CRS (e.g. EPSG: 5070)

Example Usage:
  spark-submit reproject.py "s3://azavea-data-hub/raw/*.tif" "s3://azavea-data-hub/reprojected" --dst-crs="EPSG:5070"

"""
import argparse
import os
import tempfile

from pyspark import SparkContext, SparkConf

import rasterio
from rasterio.warp import (
    calculate_default_transform,
    reproject,
    RESAMPLING
)


SAMPLING_METHODS = {
    'nearest': RESAMPLING.nearest,
    'bilinear': RESAMPLING.bilinear,
    'cubic': RESAMPLING.cubic,
    'cubic_spline': RESAMPLING.cubic_spline,
    'lanczos': RESAMPLING.lanczos,
    'average': RESAMPLING.average,
    'mode': RESAMPLING.mode
}


def cast_no_data_value(no_data_value, dtype):
    """Handles casting nodata values to correct type"""
    int_types = ['uint8', 'uint16', 'int16', 'uint32', 'int32']

    if dtype in int_types:
        return int(no_data_value)
    else:
        return float(no_data_value)


def reproject_tif(original_filepath, tif_bytes, dst_crs,
                  sampling_method, no_data_value=None):
    """Reproject tif to a destination CRS

    Reprojects a GeoTiff to a new CRS using rasterio, saving the reprojected
    GeoTiff to a new temporary location.

    Args:
      original_filepath (string): original filepath of tif on remote store
      tif_bytes (string/bytes): tif data in bytestring format
      dst_crs (string): New coordinate reference system to reproject GeoTiff. Currently
    only supports strings of the format `EPSG:XXXX`
      sampling_method (int): value derived from given rasterio sampling methods in rasterio.warp.RESAMPLING
      no_data_value (string): optional argument provided to set nodata value on reprojected tif. This is
    useful when the original tif does not have a nodata value set, but there is one that _should_ be set.
    This helps prevent issues downstream with GeoTrellis due to no fault of GeoTrellis.

    Returns:
      (string, bytes) : Returns a tuple of (string, bytes) that can then be saved off
    as a Hadoop sequenceFile or processed further

    NOTE: The `compress` and `optimize_size` options in the `reproject` function
    for `rasterio` are necessary to get around a limitation of `gdal_warp` where
    the reprojected GeoTiff is not compressed. There is a performance penalty in
    some cases, but has so far been unoticeable for the expected workload in
    Azavea Data Hub. Nevertheless, the performance penalty is likely insignifcant
    compared to the additional bandwidth/time spent moving files to/from s3 due to
    not compressing them.

    See https://trac.osgeo.org/gdal/wiki/UserDocs/GdalWarp#GeoTIFFoutput-coCOMPRESSisbroken
    for more information
    """
    raw_tif_file = tempfile.NamedTemporaryFile()
    reprojected_tif_file = tempfile.NamedTemporaryFile()

    # write temporary file
    raw_tif_file.file.write(tif_bytes)
    raw_tif_file.file.flush()
    raw_tif_file.file.seek(0)

    with rasterio.open(raw_tif_file.name) as src:
        affine, width, height = calculate_default_transform(
            src.crs, dst_crs, src.width, src.height, *src.bounds)
        kwargs = src.profile.copy()
        kwargs.update({
            'crs': dst_crs,
            'transform': affine,
            'affine': affine,
            'width': width,
            'height': height,
            'compress': 'deflate'
        })

        dtype = kwargs.get('dtype')
        src_no_data = kwargs.pop('nodata', None)

        # given no data value always overrides
        if no_data_value:
            no_data_value = cast_no_data_value(no_data_value, dtype)
        else:
            no_data_value = cast_no_data_value(src_no_data, dtype)

        with rasterio.open(reprojected_tif_file.name, 'w', nodata=no_data_value, **kwargs) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.affine,
                    src_crs=src.crs,
                    dst_transform=affine,
                    dst_crs=dst_crs,
                    src_nodata=no_data_value,
                    dst_nodata=no_data_value,
                    resampling=sampling_method,
                    optimize_size='yes' # necessary to keep size small, performance penalty
                )

    # rasterio/gdal deletes the file first, so actually need
    # need to read it rather than use reprojected_tif_file.file.read()
    with open(reprojected_tif_file.name, 'rb') as fh:
        tif_bytes = bytearray(fh.read()) # Has to be `bytearray` to serialize in Java correctly

    reprojected_tif_file.close()
    image_filename = os.path.split(original_filepath)[1]

    return (image_filename, tif_bytes)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('src_tif_dir', help='Directory with GeoTif files to reproject')
    parser.add_argument('rdd_dst', help='Directory to write Hadoop sequenceFile to')
    parser.add_argument('--dst-crs', default='EPSG:5070',
                        help='CRS to reproject GeoTif files to')
    parser.add_argument(
        '--partitions', default=250, type=int,
        help=('Number of partitions to coalesce geotiffs to else '
              'each geotiff will end up in its own partition'))
    parser.add_argument(
        '--sampling-method', default="nearest",
        choices=SAMPLING_METHODS.keys(),
        help=('Sampling method to use during reprojection')
    )
    parser.add_argument(
        '--no-data-value', default=None,
        help='Value to represent no data if not set in original geotiff'
    )

    args = parser.parse_args()

    spark_conf = SparkConf().setAppName('Azavea-Data-Hub-Reprojection')
    sc = SparkContext(conf=spark_conf)

    sampling_method = SAMPLING_METHODS.get(args.sampling_method, RESAMPLING.nearest)

    raw_tifs = sc.binaryFiles(args.src_tif_dir).coalesce(args.partitions)

    reprojected_tifs = raw_tifs.map(
        lambda (src_tif_path_remote, tif_bytes): reproject_tif(
            src_tif_path_remote, tif_bytes, args.dst_crs,
            sampling_method, args.no_data_value
        )
    )

    reprojected_tifs.saveAsSequenceFile(args.rdd_dst)
