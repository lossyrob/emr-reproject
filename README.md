# Reprojection using EMR, Pyspark and Rasterio

This project is used to reproject GeoTiffs in S3.

#### Starting place

Do you have a giant GeoTiff somewhere not on S3? Great.
Already have tiles? Skip to step 3

#### Step 1: Chunk it up.

It's not fun to work with GeoTiffs that are GB's big. Let's break it up! We'll create smaller "tiles" to allow parallel processing in Step 3. These tiles are not related to the final output tiles created in that step.

```console
> gdal_retile.py -of GTiff -co compress=deflate -ps 1024 1024 -targetDir ../tiles/the-big-file/ the-big-file.tif
```

After potentially a very long time, this will produce a directory of compressed GeoTiff tiles that is the data we'll be working with.

#### Step 2: Upload it to S3

We need to get these files on S3 for processing. Use the `awscli` to do this:

```console
aws s3 cp --recursive ../tiles/the-big-file/ s3://raster-store/raw/type/the-big-file/
```

#### Step 3: Reproject

Reprojecting tiled geotiffs is done as a pyspark job using [rasterio]().

Fill out the configuration in `emr/environment.sh`.

Call `emr/start-cluster.sh` to start the cluster. Record the returned cluster id in the appropriate place of `emr/reproject-to-s3.sh`. Fill out any other arguments in that script as well.

Once the cluster is up and running, you can run the repojection using `emr/reproject-to-s3.sh`

Don't forget to terminate the cluster!

#### How to connect to UI's in Elastic Map Reduce (EMR)

Follow these instructions: Namely set up the ssh tunnel using: http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-ssh-tunnel.html

```
ssh -i ~/mykeypair.pem -N -D 8157 hadoop@ec2-###-##-##-###.compute-1.amazonaws.com
```

Use foxyproxy to set up proxy forwarding; instructions are found here: http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-connect-master-node-proxy.html
However, use the following foxyproxy settings instead of the one listed in that doc (this enables getting to the Spark UI)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<foxyproxy>
   <proxies>
      <proxy name="emr-socks-proxy" id="56835462" notes="" enabled="true" color="#0055E5" mode="manual" autoconfMode="pac" lastresort="false">
         <manualconf host="localhost" port="8157" socksversion="5" isSocks="true" />
         <autoconf url="" reloadPac="false" loadNotification="true" errorNotification="true" autoReload="false" reloadFreqMins="60" disableOnBadPAC="true" />
         <matches>
            <match enabled="true" name="*ec2*.amazonaws.com*" isRegex="false" pattern="*ec2*.amazonaws.com*" reload="true" autoReload="false" isBlackList="false" isMultiLine="false" fromSubscription="false" caseSensitive="false" />
            <match enabled="true" name="*ec2*.compute*" isRegex="false" pattern="*ec2*.compute*" reload="true" autoReload="false" isBlackList="false" isMultiLine="false" fromSubscription="false" caseSensitive="false" />
            <match enabled="true" name="10.*" isRegex="false" pattern="http://10.*" reload="true" autoReload="false" isBlackList="false" isMultiLine="false" fromSubscription="false" caseSensitive="false" />
            <match enabled="true" name="*ec2.internal*" isRegex="false" pattern="*ec2.internal*" reload="true" autoReload="false" isBlackList="false" isMultiLine="false" fromSubscription="false" caseSensitive="false" />
            <match enabled="true" name="*compute.internal*" isRegex="false" pattern="*compute.internal*" reload="true" autoReload="false" isBlackList="false" isMultiLine="false" fromSubscription="false" caseSensitive="false" />
         </matches>
      </proxy>
   </proxies>
</foxyproxy>
```

After foxyproxy is enabled and the SSH tunnel is set up, you can click on the `Resource Manager` in the AWS UI for the cluster, which takes you to the YARN UI. For a running job, there will be an Tracking UI `Application Master` link all the way to the right for that job. Click on that, and it should bring up the Spark UI.
