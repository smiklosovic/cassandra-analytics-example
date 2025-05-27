#!/bin/bash

mvn clean install
cp src/main/resources/spark.properties submit/spark.properties && cp target/build.jar submit/analytics-app

docker exec -i spark_master_1 sh -c 'cd /submit/analytics-app; ${SPARK_HOME}/bin/spark-submit \
    --driver-memory 1G \
    --num-executors 2 \
    --executor-cores 4 \
    --executor-memory 4G \
    --total-executor-cores 8 \
    --class org.apache.cassandra.spark.analytics.example.App \
    --master spark://spark-master-1:7077 \
    --deploy-mode client \
    --properties-file /submit/spark.properties \
    --verbose \
    --conf "spark.driver.extraJavaOptions=-DSKIP_STARTUP_VALIDATIONS=true -Dfile.encoding=UTF-8 -Djdk.attach.allowAttachSelf=true --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.base/jdk.internal.ref=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED --add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED --add-exports java.rmi/sun.rmi.server=ALL-UNNAMED --add-exports java.sql/java.sql=ALL-UNNAMED --add-opens java.base/java.lang.module=ALL-UNNAMED --add-opens java.base/jdk.internal.loader=ALL-UNNAMED --add-opens java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED --add-opens java.base/jdk.internal.math=ALL-UNNAMED --add-opens java.base/jdk.internal.module=ALL-UNNAMED --add-opens java.base/jdk.internal.util.jar=ALL-UNNAMED --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED" \
    --jars cassandra-analytics-core_spark3_2.12-1.0.0.jar,cassandra-analytics-common_spark3_2.12-1.0.0.jar,cassandra-analytics-spark-converter_spark3_2.12-1.0.0.jar,cassandra-bridge_spark3_2.12-1.0.0.jar,bridges/four-zero-types.jar,bridges/four-zero-bridge.jar,bridges/four-zero.jar,bridges/four-zero-sparksql.jar,bridges/four-zero-avro.jar,netty-nio-client-2.26.12.jar,s3-2.26.12.jar \
    /submit/analytics-app/build.jar'
