# Pack

Pack is a HDFS backed block device and volume manager.  Pack is targeted to work with Docker as a Volume Plugin and Cloudera Manager for deployment.

## Building Pack
~~~~
mvn clean install -DskipTests
~~~~

## Installation

### CSD
First the CSD jar ```pack-csd/target/PACK-2.0.jar``` needs to be copied to the Cloudera Manager Server into path ```/opt/cloudera/csd/```.

Then go to the following URIs:
```
http://hostname:7180/cmf/csd/refresh
http://hostname:7180/cmf/csd/install?csdName=PACK-2.0
```

Or simply restart the Cloudera Manager process.

### Parcel
Run the parcel server script:
~~~
./run_parcel_server.sh
~~~

In Cloudera Manager now add the computer hostname and port to the Cloudera Manager -> Parcels -> Configuration -> Remote Parcel Repository URLs list.  The parcel server port is 8001.

### Cloudera Manager Setup

At this point you should be able to add Pack as a service to your cluster.  Install the Pack Agents on all the nodes where the Docker daemon is running.  Install the Pack Compactors on HDFS Datanodes (this is not a requirement).
