#!/bin/bash

# Update apt
sudo apt update

# Install java
sudo apt install openjdk-8-jre-headless
# Add java to environment variables
echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> ~/.bashrc
echo "export PATH=\$PATH:/usr/lib/jvm/java-8-openjdk-amd64/bin" >> ~/.bashrc



# Install Scala
sudo apt install scala


# Install Apache
sudo apt install apache2

# Install Spark
sudo wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
sudo tar xvf spark-3.5.1-bin-hadoop3.tgz
sudo mv spark-3.5.1-bin-hadoop3/ /opt/spark
sudo rm spark-3.5.1-bin-hadoop3.tgz

echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
echo "export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin" >> ~/.bashrc
echo "export PYTHONPATH=$(ZIPS=(""/opt/spark""/python/lib/*.zip); IFS=:; echo ""${ZIPS[*]}""):$PYTHONPATH" >> ~/.bashrc
echo "export PYSPARK_DRIVER_PYTHON=/home/yakim/.venv/bin" >> ~/.bashrc
echo "export PYSPARK_PYTHON=python" >> ~/.bashrc


# Installing Hadoop
sudo wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
sudo tar xvf hadoop-3.3.6.tar.gz
sudo mv hadoop-3.3.6/ /opt/hadoop-3.3.6
sudo rm hadoop-3.3.6.tar.gz

echo "export HADOOP_HOME=/opt/hadoop-3.3.6" >> ~/.bashrc
echo "export HADOOP_INSTALL=/opt/hadoop-3.3.6" >> ~/.bashrc
echo "export PATH=\$HADOOP_INSTALL/bin:\$HADOOP_INSTALL/sbin:\$PATH" >> ~/.bashrc
echo "export HADOOP_MAPRED_HOME=\$HADOOP_INSTALL" >> ~/.bashrc
echo "export HADOOP_COMMON_HOME=\$HADOOP_INSTALL" >> ~/.bashrc
echo "export HADOOP_HDFS_HOME=\$HADOOP_INSTALL" >> ~/.bashrc
echo "export YARN_HOME=\$HADOOP_INSTALL" >> ~/.bashrc
echo "export HADOOP_COMMON_LIB_NATIVE_DIR=\$HADOOP_INSTALL/lib/native" >> ~/.bashrc
echo "export HADOOP_OPTS='-Djava.library.path=\$HADOOP_INSTALL/lib'" >> ~/.bashrc
echo "export LD_LIBRARY_PATH=\$HADOOP_HOME/lib/native" >> ~/.bashrc
# Adding configurations to hadoop files
echo "<configuration><property><name>fs.default.name</name><value>hdfs://0.0.0.0:19000</value></property></configuration>" >> /opt/hadoop-3.3.6/etc/hadoop/core-site.xml
echo "<configuration><property><name>mapreduce.framework.name</name><value>yarn</value></property><property> <name>mapreduce.application.classpath</name><value>%HADOOP_HOME%/share/hadoop/mapreduce/*,%HADOOP_HOME%/share/hadoop/mapreduce/lib/*,%HADOOP_HOME%/share/hadoop/common/*,%HADOOP_HOME%/share/hadoop/common/lib/*,%HADOOP_HOME%/share/hadoop/yarn/*,%HADOOP_HOME%/share/hadoop/yarn/lib/*,%HADOOP_HOME%/share/hadoop/hdfs/*,%HADOOP_HOME%/share/hadoop/hdfs/lib/*</value></property></configuration>" >> /opt/hadoop-3.3.6/etc/hadoop/mapred-site.xml
echo "<configuration><property><name>dfs.replication</name><value>1</value></property><property><name>dfs.namenode.name.dir</name><value>file:////opt/hadoop-3.3.6/data/dfs/namespace_logs</value></property><property><name>dfs.datanode.data.dir</name><value>file:////opt/hadoop-3.3.6/data/dfs/data</value></property></configuration>" >> /opt/hadoop-3.3.6/etc/hadoop/hdfs-site.xml

# Install mpich - To run modin through unidist using MPI as the backend
sudo apt install mpich              # Necessary to install mpi4py
sudo apt-get install python3.9-dev    # Necessary to install mpi4py (this allows the #include <Python.h> C header)

# Install python version 3.9
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt install python3.8
# Install venv for python3.9
sudo apt-get install python3.8-venv

# Create a virtual environment with the necessary requirements
sudo python3.8 -m venv .venv

# Activate virtual environment
source .venv/bin/activate

sudo .venv/bin/python3 -m pip install -r requirements.txt --extra-index-url=https://pypi.nvidia.com

# Forcefuly try to install CUDF and dask-cudf
sudo .venv/bin/python3 -m pip install cudf-cu12==24.4.* dask-cudf-cu12==24.4.* --extra-index-url=https://pypi.nvidia.com

# Some other environment variables necessary
echo "export PYARROW_IGNORE_TIMEZONE=\"1\"" >> ~/.bashrc
