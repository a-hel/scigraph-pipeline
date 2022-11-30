#!/usr/bin/bash

# Migrate local Neo4j database to cloud
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

"$NEO4J_HOME/bin/neo4j" stop && \
"$NEO4J_HOME/bin/neo4j-admin" copy --to-database=neo4jstaging --from-database=neo4j --force && \
"$NEO4J_HOME/bin/neo4j-admin" push-to-cloud \
    --bolt-uri bolt+routing://$TARGET_HOST \
    --database neo4jstaging \
    --username $TARGET_USER \
    --password $TARGET_PASS \
    --overwrite && \
"$NEO4J_HOME/bin/neo4j-admin" copy --to-database=neo4jstaging --from-database=neo4j --force
