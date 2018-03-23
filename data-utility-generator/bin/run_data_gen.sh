#!/usr/bin/env bash

CUR_DIR=`pwd`

DATA_GEN_JAR=$HOME/.m2/repository/com/streever/iot/data/utility/data.utility.generator/3.0-SNAPSHOT/data.utility.generator-3.0-SNAPSHOT-shaded.jar

java -cp $DATA_GEN_JAR com.streever.iot.data.utility.generator.cli.RecordGenerator $@

