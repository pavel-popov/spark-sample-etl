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
  echo "Usage: run_etl.sh <source_file> <client_code> <period>"
  error "Invalid number of arguments"
fi

SOURCE_FILE=$1
CLIENT_CODE=$2
PERIOD=$3
VERSION="0.0.1-SNAPSHOT-jar-with-dependencies"
DATA_DIR=$(pwd)/data

CLASS=com.megastartup.orders.Main
SPARK_HOME=/opt/spark-1.6.1-bin-hadoop2.6

info "Spark Home: $SPARK_HOME"
info "Parameters: SOURCE_FILE=$SOURCE_FILE CLIENT_CODE=$CLIENT_CODE PERIOD=$PERIOD"

info "Creating directories structure"

create_dirs() {
  mkdir -p "$DATA_DIR/dim/customer/$1"
  touch "$DATA_DIR/dim/customer/$1/dummy.txt"
}

create_dirs "$CLIENT_CODE"

info "Cleaning up existing data"

cleanup() {
  info "Removing dimension increments"
  rm -rf "$DATA_DIR/dim/customer/$1/$PERIOD"

  info "Removing facts increments"
  rm -rf "$DATA_DIR/fact/orders/$1/$PERIOD"

  info "Removing DataMart data"
  rm -rf "$DATA_DIR/dm/orders/$1/$PERIOD"
}

cleanup "$CLIENT_CODE"

$SPARK_HOME/bin/spark-submit \
  --class $CLASS \
  --master local \
  target/orders-$VERSION.jar \
  $CLIENT_CODE $SOURCE_FILE $DATA_DIR $PERIOD 2>&1 | tee -a run.log
