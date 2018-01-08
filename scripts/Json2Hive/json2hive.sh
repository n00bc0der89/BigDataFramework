#!/bin/bash
echo "PWD " $PWD
echo "Java_Home " $JAVA_HOME

INPUT_FILE_LOCATION=$1
HDFS_PATH=$2
TABLE_NAME=$3
CREATE_HIVE_TABLE=$4

echo "INPUT_FILE_LOCATION " $INPUT_FILE_LOCATION
echo "HDFS_PATH " $HDFS_PATH
echo "TABLE_NAME " $TABLE_NAME
echo "CREATE_HIVE_TABLE " $CREATE_HIVE_TABLE

${JAVA_HOME}/bin/java -jar ${PWD}/scripts/Json2Hive/json-hive-schema-1.0-jar-with-dependencies.jar $INPUT_FILE_LOCATION $TABLE_NAME $HDFS_PATH > ${PWD}/hiveSchema/${TABLE_NAME}.hql

# Checks if the hive executable exists
HIVE_EXISTS=0
if which "hive" >/dev/null; then
        HIVE_EXISTS=1
fi

# Create the Hive table if asked
if [ "${CREATE_HIVE_TABLE}" = "1" ]; then
        if [ "${HIVE_EXISTS}" = "1" ]; then
                hive -f "${PWD}/hiveSchema/${TABLE_NAME}.hql"
        else
                echo "-> Warning: The executable 'hive' doesn't exists !"
                echo "            Don't use \"--create\" to avoid this warning the next time."
                echo "            Anyway, a Hive 'CREATE TABLE' file named \"${HIVE_TABLE_NAME}.hql\" has been generated."
        fi
fi
