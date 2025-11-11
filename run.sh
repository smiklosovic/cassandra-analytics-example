#!/bin/bash

mvn clean install -PsnapshotRepo

if [ "$?" != "0" ]; then
  exit 1
fi

cp src/main/resources/spark-five-zero.properties submit/analytics-app-five-zero/spark.properties && cp target/build.jar submit/analytics-app-five-zero/build.jar
cp src/main/resources/spark-four-zero.properties submit/analytics-app-four-zero/spark.properties && cp target/build.jar submit/analytics-app-four-zero/build.jar

CASSANDRA_VERSION=$1

if [ "x$CASSANDRA_VERSION" = "x" ]; then
  CASSANDRA_VERSION="5.0"
fi

if [ "$CASSANDRA_VERSION" != "4.0" ]; then
  if [ "$CASSANDRA_VERSION" != "5.0" ]; then
    echo "version can be either 4.0 or 5.0"
    exit 1
  fi
fi

if [ "$CASSANDRA_VERSION" = "5.0" ]; then
  docker exec -i spark_master_1 sh -c 'cd /submit/analytics-app-five-zero; ${SPARK_HOME}/bin/spark-submit \
      --driver-memory 1G \
      --executor-cores 2 \
      --executor-memory 4G \
      --total-executor-cores 6 \
      --class org.apache.cassandra.spark.analytics.example.App \
      --master spark://172.19.0.5:7077 \
      --deploy-mode client \
      --properties-file /submit/analytics-app-five-zero/spark.properties \
      --verbose \
      --conf "spark.driver.extraJavaOptions=-DSKIP_STARTUP_VALIDATIONS=true -Dfile.encoding=UTF-8 -Djdk.attach.allowAttachSelf=true --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.base/jdk.internal.ref=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED --add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED --add-exports java.rmi/sun.rmi.server=ALL-UNNAMED --add-exports java.sql/java.sql=ALL-UNNAMED --add-opens java.base/java.lang.module=ALL-UNNAMED --add-opens java.base/jdk.internal.loader=ALL-UNNAMED --add-opens java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED --add-opens java.base/jdk.internal.math=ALL-UNNAMED --add-opens java.base/jdk.internal.module=ALL-UNNAMED --add-opens java.base/jdk.internal.util.jar=ALL-UNNAMED --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED" \
      --jars netty-codec-http-4.1.118.Final.jar,analytics-sidecar-vertx-client-0.2.0.jar,cassandra-analytics-sidecar-client-0.2.0.jar,cassandra-analytics-core-0.2.0.jar,cassandra-analytics-common-0.2.0.jar,cassandra-analytics-spark-converter-0.2.0.jar,cassandra-bridge-0.2.0.jar,bridges/five-zero-types.jar,bridges/five-zero-bridge.jar,bridges/five-zero.jar,bridges/five-zero-sparksql.jar,bridges/five-zero-avro.jar,netty-nio-client-2.26.12.jar,s3-2.26.12.jar \
      /submit/analytics-app-five-zero/build.jar'
fi

if [ "$CASSANDRA_VERSION" = "4.0" ]; then
  docker exec -i spark_master_1 sh -c 'cd /submit/analytics-app-four-zero; ${SPARK_HOME}/bin/spark-submit \
      --driver-memory 1G \
      --executor-cores 2 \
      --executor-memory 4G \
      --total-executor-cores 6 \
      --class org.apache.cassandra.spark.analytics.example.App \
      --master spark://172.19.0.5:7077 \
      --deploy-mode client \
      --properties-file /submit/analytics-app-four-zero/spark.properties \
      --verbose \
      --conf "spark.driver.extraJavaOptions=-DSKIP_STARTUP_VALIDATIONS=true -Dfile.encoding=UTF-8 -Djdk.attach.allowAttachSelf=true --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.base/jdk.internal.ref=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED --add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED --add-exports java.rmi/sun.rmi.server=ALL-UNNAMED --add-exports java.sql/java.sql=ALL-UNNAMED --add-opens java.base/java.lang.module=ALL-UNNAMED --add-opens java.base/jdk.internal.loader=ALL-UNNAMED --add-opens java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED --add-opens java.base/jdk.internal.math=ALL-UNNAMED --add-opens java.base/jdk.internal.module=ALL-UNNAMED --add-opens java.base/jdk.internal.util.jar=ALL-UNNAMED --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED" \
      --jars netty-codec-http-4.1.118.Final.jar,analytics-sidecar-vertx-client-0.2.0.jar,cassandra-analytics-sidecar-client-0.2.0.jar,cassandra-analytics-core-0.2.0.jar,cassandra-analytics-common-0.2.0.jar,cassandra-analytics-spark-converter-0.2.0.jar,cassandra-bridge-0.2.0.jar,bridges/four-zero-types.jar,bridges/four-zero-bridge.jar,bridges/four-zero.jar,bridges/four-zero-sparksql.jar,bridges/four-zero-avro.jar,netty-nio-client-2.26.12.jar,s3-2.26.12.jar \
      /submit/analytics-app-four-zero/build.jar'
fi