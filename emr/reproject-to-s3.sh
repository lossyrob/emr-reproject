DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/environment.sh

CLUSTER_ID=j-2JUJIIG2KTHN7

DATA_NAME=rainfall-wm
DST_CRS=EPSG:3857
SOURCE=s3://kisma.ksat/rainfall/
TARGET=s3://ksat-test-1/rainfall-wm
REGION=eu-central-1


DRIVER_MEMORY=2g
NUM_EXECUTORS=40
EXECUTOR_MEMORY=2304m
EXECUTOR_CORES=1

aws emr add-steps \
  --region $AWS_REGION \
  --cluster-id $CLUSTER_ID \
  --steps \
    Name=REPROJECT,ActionOnFailure=CONTINUE,Type=Spark,Args=[--deploy-mode,cluster,--driver-memory,$DRIVER_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-memory,$EXECUTOR_MEMORY,--executor-cores,$EXECUTOR_CORES,$CODE_TARGET/reproject_to_s3.py,--extension,tif,--data-name,$DATA_NAME,--dst-crs,$DST_CRS,--region,$AWS_REGION,$SOURCE,$TARGET]
