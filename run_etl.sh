#!/bin/bash
set -o nounset
set -o errexit

log() {
  >&2 echo "[$1] $2"
}

info() {
  log "INFO" "$1"
}

error() {
  log "ERROR" "$1"
  exit 1
}

if [ "$#" -ne 3 ]; then
  echo "Usage: run_etl.sh <source_file> <client_id> <period>"
  error "Invalid number of arguments"
fi

SOURCE_FILE=$1
CLIENT_ID=$2
PERIOD=$3
VERSION="0.0.1-SNAPSHOT"
CLASS=org.megastartup.orders.OrdersEighty
SPARK_HOME=/opt/spark-1.6.1-bin-hadoop2.6


info "Spark Home: $SPARK_HOME"
info "Parameters: SOURCE_FILE=$SOURCE_FILE CLIENT_ID=$CLIENT_ID PERIOD=$PERIOD"


$SPARK_HOME/bin/spark-submit \
  --class $CLASS \
  --master local \
  etl/target/orders-$VERSION.jar \
  $SOURCE_FILE 2>&1 | tee -a run.log
