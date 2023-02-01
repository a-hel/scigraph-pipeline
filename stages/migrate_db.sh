#!/usr/bin/bash

# Migrate local Neo4j database to cloud

WRITE=False
OVERWRITE=False
DEV_DATABASE=neo4j
STAGING_DATABASE="neo4j-staging"

for arg in $@; do
    case $arg in
      -w | --write) 
        WRITE=$2
        shift
        shift;;
      -m | --mode)
        if [ $2 = "ALL" ]
        then
            OVERWRITE="--overwrite"
        else
            OVERWRITE=""
        fi
        shift
        shift;;
   esac
done

if [ -z `which jq` ]
then
    echo "Could not find jq."
    echo "Please install first or add to path."
    exit 1
fi

source .env

if [ -z $CONFIG_PATH ]
then
    echo "Unable to read config file:"
    echo "\$CONFIG_PATH is not defined in .env"
    exit 1
fi

TARGET_HOST=`cat $CONFIG_PATH | jq -r .neo4j_production.host`
TARGET_USER=`cat $CONFIG_PATH | jq -r .neo4j_production.username`
TARGET_PASS=`cat $CONFIG_PATH | jq -r .neo4j_production.password`
SOURCE_DB=`cat $CONFIG_PATH | jq -r .neo4j_staging.database`


if [ $WRITE = "False" ] 
then
    exit 0
fi

"$NEO4J_HOME/bin/neo4j" stop && \
"$NEO4J_HOME/bin/neo4j-admin" copy --to-database=$STAGING_DATABASE --from-database=$DEV_DATABASE --force && \
"$NEO4J_HOME/bin/neo4j-admin" push-to-cloud \
    --bolt-uri bolt+routing://$TARGET_HOST \
    --database $STAGING_DATABASE \
    --username $TARGET_USER \
    --password $TARGET_PASS \
    $OVERWRITE