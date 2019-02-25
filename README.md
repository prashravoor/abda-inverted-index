# Inverted Index formulation using TF-IDF on a Hadoop Cluster

## Cluster Configuration
Create a user called "hadoopusr" or similar, on all nodes which are to be part of the cluster. **ALL HADOOP OPERATIONS ARE PERFORMED THROUGH THIS USER** <br>
As the newly created hadoopusr, setup the node for pseudo-distributed mode as hown in the link from the apache [site](https://hadoop.apache.org/docs/r2.9.2/hadoop-project-dist/hadoop-common/SingleCluster.html). Repeat for all nodes which will form the cluster <br>
Once the setup is complete, identify one of the nodes to be a master. The remaining nodes are slaves <br>
Add the master's public key to all the slave nodes through the following commands: <br>
On Master:
```bash
cp ~/.ssh/id_rsa.pub /tmp/master
```
On each slave:
```bash
scp <master IP>:/tmp/master /tmp/master
cat /tmp/master >> ~/.ssh/authorized_keys
```

### Configure the node for operation
For each node, the following three changes need to be made. All the below files are located in the $HADOOP_HOME/etc/hadoop folder <br>
core-site.xml: Add the master node's **hostname** for the property field <br>
yarn-site.xml: Add a property for the YARN hostname, and set it to the **hostname** of the master <br>
hdfs-site.xml: Add the local disk path where the HDFS files will be stored. One property for each namenode as well as datanode. <br>
<br>
In addition to these, on the master node, add the **hostname** of each of the slaves to the slaves file. <br>

### Configure /etc/hosts
On each node, open the `/etc/hosts` file in `sudo` mode. <br>
Add entries for `IP address hostname` for each node if the cluster <br>
Uncomment the line near the top of the file which sets the loopback IP address to the hostname <br>
For e.g.:
```
localhost 127.0.0.1
# ubuntu 127.0.0.1
```

### Configure Job History Server
Configure the `yarn-site.xml` file to support log aggregation to store history of past applications run on the cluster. <br>
<br> <br>
Sample configuration files are included in the `conf` folder for each of the above <br>

## Running Hadoop Cluster
If the cluster is being setup for the first time, format the namenode through the command: <br>
`hdfs namenode -format` <br>
This needs to be run everytime there is a change to the hdfs-site.xml file. <br>

Some utility scripts are included in the `scripts` folder to start and stop the hadoop cluster. They need to be modified as needed for the cluster to run correctly. <br>
On the master node, execute the `start-all.sh` script included in the repo to start the Cluster. `stop-all.sh` will stop the cluster. <br>

If needed, additionally run the following command on each slave node to start the history server: <br>
`mr-jobhistory-daemon.sh --config $HADOOP_HOME/etc/hadoop start historyserver` <br>

## Compiling the code
The code can be compiled into a jar using the following commands:
```bash
hadoop com.sun.tools.javac.Main InvertedIndex.java -d inverted-index
jar cf InvertedIndex.jar inverted-index/*.class
```

Similarly, the same commands can be repeated for the QueryDocs.java file to create the Query.jar file <br>

## Running the code
Upload the dataset to the HDFS using the following commands:
```bash
hdfs dfs -mkdir -p /usr/input/dataset
hdfs dfs -put <path to dataset on local FS> /usr/input/dataset
```

Once the dataset is uploaded to HDFS, the map-reduce job can be started through: <br>
`hadoop jar InvertedIndex.jar InvertedIndex <HDFS Input path> <Hdfs output path>` <br>

To fetch the result, run `hdfs dfs -cat <path to output>/part-*` <br>

Once the index is built, the Query can be run using the command `run-query.sh <Inverted Index location on HDFS> <Query Output path on HDFS>` <br>
