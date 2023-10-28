#!/bin/bash

mvn package -DskipTests

rm -rf output
$SPARK_HOME/bin/spark-submit --class net.homework.App \
	./target/CalScore-1.0-SNAPSHOT.jar > ./target/res.txt

diff ./target/res.txt ./std/std0.txt
echo $?
cat ./target/res.txt
