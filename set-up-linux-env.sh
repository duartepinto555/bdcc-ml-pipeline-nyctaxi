#!/bin/bash

# Update apt
sudo apt update

# Install java
sudo apt install openjdk-8-jre-headless
# Add java to environment variables
echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> ~/.bashrc



# Install Scala
sudo apt install scala


# Install Apache
sudo apt install apache2

# Install Spark
sudo wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
sudo tar xvf spark-3.5.1-bin-hadoop3.tgz
sudo mv spark-3.5.1-bin-hadoop3/ /opt/spark

echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
echo "export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> ~/.bashrc
echo "export PYTHONPATH=$(ZIPS=(""$SPARK_HOME""/python/lib/*.zip); IFS=:; echo ""${ZIPS[*]}""):$PYTHONPATH" >> ~/.bashrc
echo "export PYSPARK_DRIVER_PYTHON=/home/yakim/.venv/bin" >> ~/.bashrc

# Install python version 3.8
sudo apt-get install python3.8

# Create a virtual environment with the necessary requirements
sudo python3.8 -m venv .venv

# Activate virtual environment
source .venv/bin/activate

sudo .venv/bin/python3 -m pip install -r requirements.txt

