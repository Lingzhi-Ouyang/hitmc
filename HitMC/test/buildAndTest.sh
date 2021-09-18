#!/bin/bash

SCRIPT_DIR=$(cd $(dirname $0);pwd)
WORKING_DIR=$(cd $SCRIPT_DIR/../..;pwd)

# build ZooKeeper
cd $WORKING_DIR/zookeeper-3.4.3 && ant

# build HitMC
cd $WORKING_DIR/HitMC && mvn install
cd $WORKING_DIR/HitMC/zookeeper-wrapper && mvn package
cd $WORKING_DIR/HitMC/zookeeper-ensemble && mvn package
cd $WORKING_DIR/HitMC/test
nohup java -jar ../zookeeper-ensemble/target/zookeeper-ensemble-jar-with-dependencies.jar zookeeper.properties > test.out 2>&1 &