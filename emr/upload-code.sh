DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/environment.sh

pushd ../
aws s3 cp ./code/reproject_to_s3.py $CODE_TARGET/reproject_to_s3.py --region $AWS_REGION
popd

aws s3 cp bootstrap.sh $EMR_TARGET/bootstrap.sh --region $AWS_REGION
aws s3 cp bootstrap-with-accumulo.sh $EMR_TARGET/bootstrap-with-accumulo.sh --region $AWS_REGION
