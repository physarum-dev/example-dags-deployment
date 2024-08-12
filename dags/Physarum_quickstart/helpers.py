import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import json
import logging
from datetime import timedelta

import re
from constants import TEST_TEAM_NAME
import os
from operators import ChrononOperator, PythonSensor, NamedHivePartitionSensor, SensorWithEndDate, create_skip_operator

from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor

time_parts = ["ds", "ts", "hr"]


def should_schedule(conf, mode, conf_type):
    """Based on a conf and mode determine if a conf should define a task."""
    if conf_type == "group_bys":
        if mode == "backfill":
            return conf.get("backfillStartDate") is not None
        if mode == "upload":
            return conf["metaData"].get("online", 0) == 1
        if mode == "streaming":
            # online + (realtime or has topic)
            online = conf["metaData"].get("online", 0) == 1
            streaming = requires_streaming_task(conf, conf_type)
            return conf["metaData"].get("online", 0) == 1 and (
                    conf["metaData"].get("accuracy", 1) == 0 or
                    requires_streaming_task(conf, conf_type)
            )
    if conf_type == "joins":
        if mode == "metadata-upload":
            return True
        if mode == "backfill":
            return requires_frontfill(conf)
        if mode == 'stats-summary':
            return requires_frontfill(conf)
        if mode == "consistency-metrics-compute":
            customJson = json.loads(conf["metaData"]["customJson"])
            return customJson.get("check_consistency") is True
        if mode == "log-flattener":
            return requires_log_flattening_task(conf)
        return False
    if conf_type == "staging_queries":
        return mode == "backfill"
    logging.warning(f"[Chronon][Schedule] Ignoring task for: {mode} {conf_type} {conf['metaData']['name']}")


def requires_streaming_task(conf, conf_type):
    """Find if there's topic or mutationTopic for a source helps define streaming tasks"""
    if conf_type == "group_bys":
        return any([
            source.get("entities", {}).get("mutationTopic") is not None or
            source.get("events", {}).get("topic") is not None
            for source in conf["sources"]
        ])
    return False


def requires_frontfill(conf):
    return get_offline_schedule(conf) is not None


def get_offline_schedule(conf):
    schedule_interval = conf["metaData"].get("offlineSchedule", "@daily")
    if schedule_interval == "@never":
        return None
    return schedule_interval


def requires_log_flattening_task(conf):
    return conf["metaData"].get("samplePercent", 0) > 0


def task_names(conf, mode, conf_type):
    if conf_type == "joins" and mode == "metadata-upload":
        return f"chronon_join_metadata_upload"
    name = normalize_name(conf["metaData"]["name"])
    # Group By Tasks
    if conf_type == "group_bys":
        if mode == "upload":
            return f"group_by_batch__{name}"
        if mode == "backfill":
            return f"group_by_batch_backfill__{name}"
        if mode == "streaming":
            return f"group_by_streaming__{name}"
    # Join Tasks
    if conf_type == "joins":
        if mode == "backfill":
            return f"compute_join__{name}"
        if mode == "consistency-metrics-compute":
            return f"compute_consistency__{name}"
        if mode == "stats-summary":
            return f"feature_stats__{name}"
        if mode == "log-flattener":
            return f"log_flattening__{name}"
    # Staging Query
    if conf_type == "staging_queries":
        if mode == "backfill":
            return f"staging_query__{name}"
    raise ValueError(
        f"Unable to define proper task name:\nconf_type: {conf_type}\nmode: {mode}\nconf: {json.dumps(conf, indent=2)}"
    )


def dag_names(dag_id, conf, mode, conf_type):
    if conf_type == "joins" and mode == "metadata-upload":
        return f"{dag_id}_metadata_upload"
    if mode == "metadata-export":
        return f"{dag_id}_ums_metadata_export"
    team = conf["metaData"]["team"]
    name = normalize_name(conf["metaData"]["name"])
    # Group By
    if conf_type == "group_bys":
        if mode in ("upload", "backfill"):
            return f"{dag_id}_group_by_batch_{team}"
        if mode == "streaming":
            return f"{dag_id}_group_by_streaming_{team}"
    # Join
    if conf_type == "joins":
        if mode == "backfill":
            return f"{dag_id}_join_{name}"
        if mode == "stats-summary":
            return f"{dag_id}_stats_compute"
        if mode == "log-flattener":
            return f"{dag_id}_log_flattening_{team}"
        if mode == "consistency-metrics-compute":
            return f"{dag_id}_online_offline_comparison_{name}"
    # Staging Query
    if conf_type == "staging_queries":
        if mode == "backfill":
            return f"{dag_id}_staging_query_batch_{team}"
    raise ValueError(
        f"Unable to define proper DAG name:\nconf_type: {conf_type}\nmode: {mode}\nconf: {json.dumps(conf, indent=2)}")


def get_extra_args(mode, conf_type, common_env, conf):
    args = {}
    if conf_type == "joins" and mode == "log-flattener":
        args.update({
            "log-table": common_env["CHRONON_LOG_TABLE"],
            "schema-table": common_env["CHRONON_SCHEMA_TABLE"]
        })
    if conf["metaData"]["team"] == TEST_TEAM_NAME:
        args.update({
            "online-jar-fetch": os.path.join("scripts/fetch_online_staging_jar.py"),
        })
    return args


def normalize_name(object_name):
    """Eliminate characters that would be problematic on task names"""

    def safe_part(p):
        return not any([
            p.startswith("{}=".format(time_part))
            for time_part in time_parts
        ])

    safe_name = "__".join(filter(safe_part, object_name.split("/")))
    return re.sub("[^A-Za-z0-9_]", "__", safe_name)


def extract_dependencies(conf, mode, conf_type, common_env, dag):
    if conf_type == 'joins' and mode == "stats-summary":
        # dependencies = [{
        #     "name": f"wf_{sanitize(output_table(conf['metaData']))}",
        #     "spec": f"{output_table(conf['metaData'])}/ds={{{{ ds }}}}",
        # }]
        pass

    elif mode == "consistency-metrics-compute":
        # table_name = logged_table(conf["metaData"])
        # dependencies = [{'name': 'wf_flattened_log_table', 'spec': f'{table_name}/ds={{{{ ds }}}}'}]
        pass

    elif mode == "streaming":
        # Streaming has no dependencies as it's purpose is a check if job is alive.
        return []
    elif conf_type == "staging_queries":
        # Staging Queries have special dependency syntax.
        dependencies = [{"name": f"wf__{dep}", "spec": dep} for dep in conf["metaData"].get("dependencies", [])]
    elif conf_type == "joins" and mode == "log-flattener":
        dependencies = [
            {
                # wait for SCHEMA_PUBLISH_EVENT partition which guarantees to exist every day
                'name': f'wf_raw_log_table',
                'spec': f'{common_env["CHRONON_LOG_TABLE"]}/ds={{{{ ds }}}}/name=SCHEMA_PUBLISH_EVENT',
            },
            {
                'name': 'wf_schema_table',
                'spec': f'{common_env["CHRONON_SCHEMA_TABLE"]}/ds={{{{ ds }}}}'
            }
        ]
    else:
        dependencies = [
            json.loads(dep) for dep in conf["metaData"].get('dependencies', [])
        ]
    operators = set()
    for dep in dependencies:
        if dep:
            name = normalize_name(dep["name"])
            if name in dag.task_dict:
                operators.add(dag.task_dict[name])
                continue
            op = SensorWithEndDate(
                task_id=name,
                partition_names=[dep["spec"]],
                params={"end_partition": dep["end"]},
                execution_timeout=timedelta(hours=48),
                dag=dag,
                retries=3,
            ) if dep.get("end") else NamedHivePartitionSensor(
                task_id=name,
                partition_names=[dep["spec"]],
                execution_timeout=timedelta(hours=48),
                dag=dag,
                retries=3,
            )
            operators.add(op)
    skip_name = f"custom_skip__{normalize_name(conf['metaData']['name'])}"
    if skip_name in dag.task_dict:
        return dag.task_dict[skip_name]
    custom_skip_op = create_skip_operator(dag, normalize_name(conf["metaData"]["name"]))

    # make a list of task from operatos set, as >> can't be applied to set
    tasks = list(operators)
    tasks >> custom_skip_op
    # operators >> custom_skip_op
    return custom_skip_op


def retry(retries=3, backoff=20):
    """ Same as open source run.py """

    def wrapper(func):
        def wrapped(*args, **kwargs):
            attempt = 0
            while attempt <= retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    logging.exception(e)
                    sleep_time = attempt * backoff
                    logging.info(
                        "[{}] Retry: {} out of {}/ Sleeping for {}"
                        .format(func.__name__, attempt, retries, sleep_time))
                    time.sleep(sleep_time)
            return func(*args, **kwargs)

        return wrapped

    return wrapper


def task_names(conf, mode, conf_type):
    if conf_type == "joins" and mode == "metadata-upload":
        return f"chronon_join_metadata_upload"
    name = normalize_name(conf["metaData"]["name"])
    # Group By Tasks
    if conf_type == "group_bys":
        if mode == "upload":
            return f"group_by_batch__{name}"
        if mode == "backfill":
            return f"group_by_batch_backfill__{name}"
        if mode == "streaming":
            return f"group_by_streaming__{name}"
    # Join Tasks
    if conf_type == "joins":
        if mode == "backfill":
            return f"compute_join__{name}"
        if mode == "consistency-metrics-compute":
            return f"compute_consistency__{name}"
        if mode == "stats-summary":
            return f"feature_stats__{name}"
        if mode == "log-flattener":
            return f"log_flattening__{name}"
    # Staging Query
    if conf_type == "staging_queries":
        if mode == "backfill":
            return f"staging_query__{name}"
    raise ValueError(
        f"Unable to define proper task name:\nconf_type: {conf_type}\nmode: {mode}\nconf: {json.dumps(conf, indent=2)}"
    )

def json_writer(json_path, data):
    """
    Write data to a JSON file.

    Parameters
    ----------
    json_path : str
        Path to the JSON file.
    data : dict
        Data to write to the JSON file.
    """
    try:
        with open(json_path, 'w') as thrift_config_file:
            json.dump(data, thrift_config_file, indent=4)
    except Exception as e:
        raise IOError(f"Failed to write JSON data to {json_path}") from e