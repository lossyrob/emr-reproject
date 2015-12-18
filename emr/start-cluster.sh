# Starts a long running ETL cluster.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/environment.sh

aws emr create-cluster \
  --name "KSAT ETL" \
  --region $AWS_REGION \
  --log-uri $EMR_TARGET/logs/ \
  --release-label emr-4.0.0 \
  --use-default-roles \
  --ec2-attributes KeyName=$KEY_NAME,SubnetId=$SUBNET_ID \
  --applications Name=Spark \
  --instance-groups \
    Name=Master,InstanceCount=1,InstanceGroupType=MASTER,InstanceType=$MASTER_INSTANCE \
    Name=Workers,InstanceCount=$WORKER_COUNT,BidPrice=$WORKER_PRICE,InstanceGroupType=CORE,InstanceType=$WORKER_INSTANCE \
  --bootstrap-action Path=$EMR_TARGET/bootstrap.sh \
  --configurations file://./emr.json
