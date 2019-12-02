#!/usr/bin/env bash

set -xe

source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/common.sh
source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/prometheus/test-correctness.sh
REVISION=$(git rev-parse HEAD)
COMPOSE_FILE=$GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/coordinator_config_rules/docker-compose.yml
# quay.io/m3db/prometheus_remote_client_golang @ v0.4.3
PROMREMOTECLI_IMAGE=quay.io/m3db/prometheus_remote_client_golang@sha256:fc56df819bff9a5a087484804acf3a584dd4a78c68900c31a28896ed66ca7e7b
JQ_IMAGE=realguess/jq:1.4@sha256:300c5d9fb1d74154248d155ce182e207cf6630acccbaadd0168e18b15bfaa786
METRIC_NAME_TEST_TOO_OLD=foo
METRIC_NAME_OLD=old_metric
METRIC_NAME_NEW=new_metric
export REVISION

echo "Pull containers required for test"
docker pull $PROMREMOTECLI_IMAGE
docker pull $JQ_IMAGE

echo "Run m3dbnode and m3coordinator containers"
docker-compose -f ${COMPOSE_FILE} up -d dbnode01
docker-compose -f ${COMPOSE_FILE} up -d coordinator01

function defer {
  docker-compose -f ${COMPOSE_FILE} down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
}
trap defer EXIT

setup_single_m3db_node

echo "Start Prometheus containers"
docker-compose -f ${COMPOSE_FILE} up -d prometheus01

function prometheus_remote_write {
  local metric_name=$1
  local datapoint_timestamp=$2
  local datapoint_value=$3
  local expect_success=$4
  local expect_success_err=$5
  local expect_status=$6
  local expect_status_err=$7
  local metrics_type=$8
  local metrics_storage_policy=$9

  network=$(docker network ls --format '{{.ID}}' | tail -n 1)
  out=$((docker run -it --rm --network coordinator_config_rules_backend          \
    $PROMREMOTECLI_IMAGE                                  \
    -u http://coordinator01:7201/api/v1/prom/remote/write \
    -t __name__:${metric_name}                            \
    -t foo:bar                            \
    -h "M3-Metrics-Type: ${metrics_type}"                 \
    -h "M3-Storage-Policy: ${metrics_storage_policy}"     \
    -d ${datapoint_timestamp},${datapoint_value} | grep -v promremotecli_log) || true)
  success=$(echo $out | grep -v promremotecli_log | docker run --rm -i $JQ_IMAGE jq .success)
  status=$(echo $out | grep -v promremotecli_log | docker run --rm -i $JQ_IMAGE jq .statusCode)
  if [[ "$success" != "$expect_success" ]]; then
    echo $expect_success_err
    sleep 10000000
    return 1
  fi
  if [[ "$status" != "$expect_status" ]]; then
    echo "${expect_status_err}: actual=${status}"
    sleep 10000000
    return 1
  fi
  echo "Returned success=${success}, status=${status} as expected"
  return 0
}


function test_prometheus_remote_write_restrict_metrics_type {
  # Test we can specify metrics type
  echo "Test write with unaggregated metrics type works as expected"
  prometheus_remote_write \
    $METRIC_NAME_NEW now 42.42 \
    true "Expected request to succeed" \
    200 "Expected request to return status code 200" \
    unaggregated
  
#   echo "Test write with aggregated metrics type works as expected"
#   prometheus_remote_write \
#     $METRIC_NAME_OLD now 84.84 \
#     true "Expected request to succeed" \
#     200 "Expected request to return status code 200" \
#     aggregated 5s:10h
}


function prometheus_query_native {
  local endpoint=${endpoint:-}
  local query=${query:-}
  local params=${params:-}
  local metrics_type=${metrics_type:-}
  local metrics_storage_policy=${metrics_storage_policy:-}
  local jq_path=${jq_path:-}
  local expected_value=${expected_value:-}

  params_prefixed=""
  if [[ "$params" != "" ]]; then
    params_prefixed='&'"${params}"
  fi

  result=$(curl -s                                    \
    -H "M3-Metrics-Type: ${metrics_type}"             \
    -H "M3-Storage-Policy: ${metrics_storage_policy}" \
    "0.0.0.0:7201/api/v1/${endpoint}?query=${query}${params_prefixed}" | jq -r "${jq_path}")
  test "$result" = "$expected_value"
  return $?
}

function test_query_new_name {
  now=$(date +"%s")
  hour_ago=$(expr $now - 3600) 
  step="30s"
  params_instant=""
  params_range="start=${hour_ago}"'&'"end=${now}"'&'"step=30s"
  jq_path_instant=".data.result[0].value[1]"
  jq_path_range=".data.result[0].values[][1]"
  
  # # Test metric name is changed
  # echo "Test query restrict to unaggregated metrics type (instant)"
  # ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 \
  #   endpoint=query query="$METRIC_NAME_NEW" params="$params_instant" \
  #   metrics_type="unaggregated" jq_path="$jq_path_instant" expected_value="42.42" \
  #   retry_with_backoff prometheus_query_native
  # echo "Test query restrict to unaggregated metrics type (range)"
  # ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 \
  #   endpoint=query_range query="$METRIC_NAME_NEW" params="$params_range" \
  #   metrics_type="unaggregated" jq_path="$jq_path_range" expected_value="42.42" \
  #   retry_with_backoff prometheus_query_native

  # Test restricting to aggregated metrics
  # echo "Test query restrict to aggregated metrics type (instant)"
  # ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 \
  #   endpoint=query query="$METRIC_NAME_NEW" params="$params_instant" \
  #   metrics_type="aggregated" metrics_storage_policy="5s:10h" jq_path="$jq_path_instant" expected_value="84.84" \
  #   retry_with_backoff prometheus_query_native
  echo "Test query restrict to aggregated metrics type (range)"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 \
    endpoint=query_range query="$METRIC_NAME_NEW" params="$params_range" \
    metrics_type="aggregated" metrics_storage_policy="5s:10h" jq_path="$jq_path_range" expected_value="84.84" \
    retry_with_backoff prometheus_query_native
}

echo "Running rule config tests..."
test_prometheus_remote_write_restrict_metrics_type
test_query_new_name
