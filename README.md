
=============

This project contains code that will allow cTAKES to be invoked on clinical document data presented as Tuples to [cTAKES](http://ctakes.apache.org).  There are two UDF's:


# Deploying to the CDH 6

## Preparation

Because this code should be run as a non-privileged user, one must first be created on the sandbox, and in HDFS.

	# useradd -G hdfs,hadoop  ctakesuser
	# passwd ctakesuser
	# su - hdfs -c "hdfs dfs -mkdir /user/ctakesuser"
	# su - hdfs -c "hdfs dfs -chown ctakesuser:hadoop /user/ctakesuser"
	# su - hdfs -c "hdfs dfs -chmod 755 /user/ctakesuser"
	
We'll also need to add SVN,MVN,GIT to checkout the Apache cTAKES code.
	# yum -y install svn
	# yum -y install maven
	# yum install git
	
## Install Hadoop CTakes
	# su - ctakesuser 
	$ mvn -version
	$ mkdir ~/src
	$ cd ~/src
	$ git clone https://github.com/wadkars/hadoop2_ctakes.git
	$ cd hadoop2_ctakes
	$ cd ~/src
	$ svn co https://svn.apache.org/repos/asf/ctakes/trunk/@1499008
	$ cd trunk
	$ mvn -Dmaven.test.skip=true install
	$ mvn clean
	$ cd ~/src
	$ rm –rf trunk	
	$ cd ~/src/hadoop2_ctakes
	$ mkdir /tmp/ctakes_config
	$ cp -r ctakes_config/* /tmp/ctakes_config
	$ chmod -R 777 /tmp/ctakes_config
	$ cd ~/src/hadoop_ctakes
	$ mvn clean install
	

## Creating the HCatalog tables

	$ hive
	hive> CREATE TABLE ctakes_annotated_docs_dummy(title STRING, PARSED BOOLEAN, text STRING, annotations STRING) PARTITIONED BY (loaded STRING) STORED AS SEQUENCEFILE;
	hive> CREATE TABLE ctakes_annotated_docs(title STRING, PARSED BOOLEAN, text STRING, annotations STRING) PARTITIONED BY (loaded STRING) STORED AS SEQUENCEFILE;
	hive> quit;

	
## Copy Sample Data Into Cluster

A few sample articles are included in the project under ./sample_data/data .  We'll add this data to the cluster using the following commands.

	$ hdfs dfs -mkdir ./sample_data_txt
	$ hdfs dfs -put ~/src/hadoop2_ctakes/sample_data/data/* ./sample_data_txt
	$ hdfs dfs -ls ./sample_data_txt
## Copy /tmp/ctakes_config to all datanodes in the cluster

The PIG UDF's will run on the data nodes. Copy the folder "ctakes_config" to all the data nodes in the cluster. My command to do that is- 

	$ scp -r /tmp/ctakes_config <user_name>@<server_name>:/tmp/


## Running the Pig Scripts on Sample Data

To create an area in which we can stage our Pig scripts and dependent Jars, we are going to create a pig directory and copy in our scripts and jars to it.

	$ mkdir ~/pig
	$ cd ~/pig
	$ cp ~/src/hadoop2_ctakes/pig/* .
	$ chmod 755 *.sh
	$ cp ~/src/hadoop2_ctakes/target/hadoop2_ctakes-0.0.1-SNAPSHOT-jar-with-dependencies.jar .
	$ cp ~/src/hadoop2_ctakes/lib/*.jar .
	
## Running the Pig Scripts on Sample Data
First convert all the small files into a smaller set of sequence files

	$ cd ~/pig
	$ ./convert_to_sequence_files.sh ./sample_data_txt ./sample_data_seq
	$ ./process_ctakes_hive.sh ./sample_data_seq/ default.ctakes_annotated_docs_dummy default.ctakes_annotated_docs



The pig job will run to completion and let you know that 10 records were written.  Now we'll make sure everything looks as it should, and confirm that the pages were parsed and placed in our wikipedia_pages table.

	$ hive -e 'select annotations from default.ctakes_annotated_docs'
	…
	<annotations here>
	Time taken: 11.106 seconds, Fetched: 10 row(s)

