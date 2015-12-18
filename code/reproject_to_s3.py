#!/usr/bin/env python

"""Reprojection Spark job for GeoTiffs

This file contains a script that can be used via `spark-submit` to handle
reprojecting GeoTif files using Spark and rasterio.

Required parameters include a source directory for unprojected GeoTifs (either local or remote) and
a destination directory for the newly projected GeoTifs.

Optional parameters include
 - destination CRS (e.g. EPSG: 5070)
 - data name (identifier for dataset)

Example Usage:
  spark-submit reproject.py "s3://raster-store/raw/*.tif" "s3://raster-store/reprojectes/*.tif" --data-name="reproj" --dst-crs="EPSG:5070"

"""
import argparse
import errno
import os
import re
import shutil
import tempfile
from urlparse import urlparse

import boto3
from botocore.client import Config

import rasterio
from rasterio.warp import (
    calculate_default_transform,
    reproject,
    RESAMPLING
)


def save_tif_for_local_processing(original_filepath, tif_bytes):
    """Saves bytes locally from RDD

    `sparkContext.binaryFiles()` returns an RDD of (filepath, bytes).
    In order to use `rasterio` the bytes need to be written locally because
    it does not support manipulating a raster from reading raw bytes. This
    function writes the bytes to a file for subsequent processing.

    NOTE: When saved locally the _very_ next step in the transformation
    needs to take the GeoTiff in order to ensure that the file is local. The
    manipulation _cannot_ be in a separate transformation.

    Args:
      original_filepath (string): original filepath where spark downloaded bytes from
      tif_bytes (bytes): byte representation of GeoTif file

    Returns:
      abs_path (string): local filepath where GeoTiff has been written to
    """
    image_filename = os.path.split(original_filepath)[1]
    _, abs_path = tempfile.mkstemp(suffix="-" + image_filename)

    with open(abs_path, 'wb') as fh:
        fh.write(tif_bytes)

    return abs_path


def save_tif_after_reprojection(src_tif_path, dst_dir, region):
    """Saves tif either remotely or locally after reprojection/processing

    This function is to get around making a HadoopOutputFormat for an RDD
    of GeoTiffs. It is able to handle either s3 URLS (s3://) or a local filepath

    Args:
      src_tif_path (string): local filepath of GeoTiff to save off either to a
    more permanent local path (e.g. HDFS) or remote path (e.g. S3)
      dst_dir (string): remote or local directory to upload file located at `src_tif_path`

    Returns:
      (string) destination path for tif
    """
    filename = os.path.split(src_tif_path)[1]
    parsed_target = urlparse(dst_dir)

    def copy_local(target):
        try:
            os.makedirs(target)
        except os.error as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(target):
                pass
            else:
                raise
        shutil.copyfile(src_tif_path, os.path.join(target, filename))

    if parsed_target.scheme == "s3":
        # TODO: Move region name out to parameter
        if region:
            client = boto3.client("s3", config=Config(region_name="eu-central-1"))
        else:
            client = boto3.client("s3")

        bucket = parsed_target.netloc
        key = os.path.join(parsed_target.path, filename)[1:]

        client.upload_file(
            src_tif_path,
            bucket,
            key
        )
    elif parsed_target.scheme == "file":
        copy_local(parsed_target.path)
    else:
        copy_local(dst_dir)
    return os.path.join(dst_dir)


def reproject_tif(src_tif_path_remote, src_tif_path, data_name, dst_crs):
    """Reproject tif to a destination CRS

    Reprojects a GeoTiff to a new CRS using rasterio, saving the reprojected
    GeoTiff to a new temporary location.

    Args:
      src_tif_path_remote (string): remote path of the tiff
      src_tif_path (string): local directory of unprocessed GeoTif
      data_name (string): string used to prefix new GeoTiffs to keep them somewhat
    identifiable
      dst_crs (string): New coordinate reference system to reproject GeoTiff. Currently
    only supports strings of the format `EPSG:XXXX`

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
    regex = r"""3IMERG\.(\d\d\d\d)(\d\d)(\d\d)"""
    regex2 = r"""(\d\d\d\d)\.(\d\d)\.(\d\d)"""

    filename = os.path.basename(src_tif_path_remote)

    # Get the date
    m = re.search(regex, filename)
    if not m:
        m = re.search(regex2, filename)
    if not m:
        raise Exception("NO TIME in %s" % src_tif_path_remote)

    basename = "3IMERG.%s%s%s" % (m.group(1), m.group(2), m.group(3))
    prefix = '-'.join(filter(lambda x: x, [data_name, basename]))
    _, dst_tif_path = tempfile.mkstemp(prefix=prefix, suffix=".tif")
    

    with rasterio.open(src_tif_path) as src:
        
        ## HACKED IN CODE
        ## We need to clip the tiles so that they can fit inside the EPSG:3857 world bounds
        ## We'll do so by first clipping the bounds, and then computin the transform

        xmin, ymin, xmax, ymax = (-180.0, -85.06, 180.0, 85.06)        
        cell_width = (src.bounds.right - src.bounds.left) / src.width
        cell_height = (src.bounds.top - src.bounds.bottom) / src.height
        clipped_width = (xmax - xmin) / cell_width
        clipped_height = (ymax - ymin) / cell_height
        
        affine, width, height = calculate_default_transform(
            src.crs, dst_crs, clipped_width, clipped_height, xmin, ymin, xmax, ymax)

        kwargs = src.meta.copy()
        kwargs.update({
            'crs': dst_crs,
            'transform': affine,
            'affine': affine,
            'width': width,
            'height': height,
            'compress': 'deflate'
        })

        with rasterio.open(dst_tif_path, 'w', **kwargs) as dst:
            dst.update_tags(**src.tags())
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.affine,
                    src_crs=src.crs,
                    dst_transform=affine,
                    dst_crs=dst_crs,
                    resampling=RESAMPLING.nearest,
                    optimize_size='yes' # necessary to keep size small, performance penalty
                )
    return dst_tif_path


def process_tif(src_tif_path_remote, image_bytes, data_name, dst_crs, dst_path, region):
    """Wrapper function that encapsulates the RDD transformation to process a GeoTiff

    High-level function that handles saving a tif locally, reprojecting it, saving
    off the reprojected tif to a new permanent local directory or S3 bucket, and
    cleaning up all local files in the process.

    Args:
      src_tif_path_remote (string): original location of downloaded tif
      image_bytes (bytes): raw bytes of GeoTiff downloaded into RDD using `sparkContext.binaryFiles()`
      data_name (string): identifable string for set of GeoTiffs being processed
      dst_crs (string): EPSG identifier (e.g. `EPSG:XXXX`) to transform GeoTiff to
      dst_path (string): directory to save new reprojected GeoTiffs to

    Returns:
      (string): new location of GeoTiff (e.g. S3 URI or local filepath)
    """
    src_tif_path = save_tif_for_local_processing(src_tif_path_remote, image_bytes)
    dst_tif_path_local = None

    try:
        dst_tif_path_local = reproject_tif(src_tif_path_remote, src_tif_path, data_name, dst_crs)
        dst_tif_path_remote = save_tif_after_reprojection(dst_tif_path_local, dst_path, region)
    finally:
        os.remove(src_tif_path)
        if dst_tif_path_local:
            os.remove(dst_tif_path_local)
    return dst_tif_path_remote


if __name__ == '__main__':
    from pyspark import SparkContext, SparkConf

    parser = argparse.ArgumentParser()

    parser.add_argument('src_tif_dir', help='Directory with files to reproject')
    parser.add_argument('dst_dir', help='Directory to write reproject files')
    parser.add_argument('--data-name', help='Optional identifer to prefix files with', default='')
    parser.add_argument('--dst-crs', help='CRS to reproject files to', default='EPSG:3857')
    parser.add_argument('--extension', help='Only consider files ending in this extension', default='')
    parser.add_argument('--region', help='Region for the S3 client to use', default='')

    args = parser.parse_args()

    spark_conf = SparkConf().setAppName('Rainfall-Reprojection')
    sc = SparkContext(conf=spark_conf)

    raw_tifs = sc.binaryFiles(args.src_tif_dir)

    if args.extension:
        raw_tifs = raw_tifs.filter(lambda (path, _): path.endswith(args.extension))

    reprojected_tifs = raw_tifs.map(
        lambda (src_tif_path_remote, tif_bytes): process_tif(
            src_tif_path_remote, tif_bytes, args.data_name, args.dst_crs, args.dst_dir, args.region
        )
    )

    num_reprojected = reprojected_tifs.count()
