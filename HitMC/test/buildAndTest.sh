#!/bin/bash

## kill current running zookeeper processes
ps -ef | grep zookeeper | grep -v grep | awk '{print $2}' | xargs kill -9

SCRIPT_DIR=$(cd $(dirname $0);pwd)
WORKING_DIR=$(cd $SCRIPT_DIR/../..;pwd)

## build ZooKeeper
#cd $WORKING_DIR/zookeeper-3.4.3 && ant

# build HitMC
cd $WORKING_DIR/HitMC && mvn install
cd $WORKING_DIR/HitMC/zookeeper-wrapper && mvn package
cd $WORKING_DIR/HitMC/zookeeper-ensemble && mvn package
cd $WORKING_DIR/HitMC/test
#rm -fr 1

tag=`date "+%y-%m-%d-%H-%M-%S"`
mkdir $tag
cp zk_log.properties $tag
nohup java -jar ../zookeeper-ensemble/target/zookeeper-ensemble-jar-with-dependencies.jar zookeeper.properties $tag > $tag/$tag.out 2>&1 &