import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import yaml
import json

from datetime import datetime, timedelta
from airflow.models import DAG

from os.path import abspath
from os.path import join as pjoin

from operators import ChrononOperator
from helpers import should_schedule, task_names, get_extra_args, extract_dependencies, dag_names, json_writer

absolute_path = abspath(os.path.dirname(__file__))

CONFIG_PATH = pjoin(absolute_path, "config.yaml")


class DagConstructor(object):

    def __init__(self, airflow_config, airflow_dag_config, feature_config, operators_config=None):

        self.airflow_config = airflow_config
        self.dag_config = airflow_dag_config
        self.feature_config = feature_config
        self.operators_config = operators_config

        self.airflow_config["dagrun_timeout"] = timedelta(seconds=self.airflow_config.pop("dagrun_timeout"))
        self.dag_config["start_date"] = datetime.strptime(self.dag_config["start_date"], "%Y-%m-%d %H:%M:%S")

    def add_task(self, mode, conf_type, dag_constructor, dags=None):

        base = pjoin(absolute_path, "thrift", conf_type)
        if not os.path.exists(base):
            return dags
        self.BASE_THRIFT = base
        if not dags: dags = {}
        root = list(os.walk(self.BASE_THRIFT))
        for root, dirs, files in os.walk(self.BASE_THRIFT):
            for name in files:
                full_path = os.path.join(root, name)
                with open(full_path, 'r') as infile:
                    try:
                        conf = json.load(infile)
                        assert (
                                conf.get('metaData', {}).get("name") is not None and
                                conf.get('metaData', {}).get("team") is not None
                        )

                        if should_schedule(conf, mode, conf_type):
                            if self.dag_config['dag_id'] not in dags:
                                dags[self.dag_config['dag_id']] = dag_constructor(
                                    dag_names(self.dag_config["dag_id"], conf, mode, conf_type))
                            dag = dags[self.dag_config['dag_id']]
                            params = self._generate_params(conf, conf_type, mode, full_path)

                            common_env = self._get_chronon_operator_conf()["env"]

                            operator = ChrononOperator(
                                conf_path=full_path,
                                mode=mode,
                                conf_type=conf_type,
                                extra_args=get_extra_args(mode, conf_type, common_env, conf),
                                task_id=task_names(conf, mode, conf_type),
                                params=params
                            )

                            dependencies = extract_dependencies(conf, mode, conf_type, common_env, dag)
                            dependencies >> operator

                            # downstream = get_downstream(conf, mode, conf_type, self.airflow_config, dag)
                            # if downstream:
                            #     operator >> downstream
                    except json.JSONDecodeError as e:
                        print(f"Ignoring invalid json file: {name}")
                    except AssertionError as x:
                        print(f"[Chronon] Ignoring {conf_type} config as does not have required metaData: {name}")
        return dags

    def _generate_params(self, conf, conf_type, mode, conf_path):
        return {
            "production": conf["metaData"].get("production", False),
            "name": conf["metaData"]["name"],
            "team": conf["metaData"]["team"],
            "conf": conf,
            "conf_path": conf_path,
            "team_conf": self._generate_team_conf(conf['metaData']['team']),
            "conf_type": conf_type,
            "task_id": task_names(conf, mode, conf_type),
        }

    def _generate_team_conf(self, team):
        conf = self._get_chronon_operator_conf()
        team_conf = {
           "default": {"common_env": {k: v for k, v in conf["env"].items() if v is not None}},
           f"{team}": conf["resources"]
        }
        json_writer(pjoin(absolute_path, "teams.json"), team_conf)
        return team_conf

    def _get_chronon_operator_conf(self):
        return self.operators_config["chronon_operator"]

    def _create_groupby_dag(self, mode=None, dags=None):

        if mode == "streaming":
            constructor = self._streaming_costructor
        else:
            constructor = self._batch_constructor

        all_dags = self.add_task(mode, "group_bys", constructor, dags)
        return all_dags

    def _create_join_dag(self, mode, dags):
        return self.add_task(mode, "joins", self._join_constructor, dags)

    def _streaming_costructor(self, dag_id):

        if self.airflow_default_args["queue"] != "silver_medium":
            self.airflow_default_args["queue"] = "silver_medium"
        # TODO: streaming constructor validators
        return DAG(
            **self.dag_default_args(dag_id),
            default_args=self.airflow_config,

        )

    def _batch_constructor(self, dag_id):
        return DAG(
            **self.dag_default_args(dag_id),
            default_args=self.airflow_config,
            max_active_runs= 1,
            max_active_tasks=1
        )

    def _join_constructor(self, dag_id):

        return DAG(
            **self.dag_default_args(dag_id),
            default_args=self.airflow_config,
            max_active_runs= 1,
            max_active_tasks=1
        )

    def dag_default_args(self, dag_id):
        return {
            'dag_id': dag_id,
            'description': self.dag_config["description"],
            'start_date': self.dag_config["start_date"],
            'schedule_interval': self.dag_config["schedule_interval"],
            'tags': self.dag_config["tags"],
        }

    def generate_dag(self):
        mode = "backfill"
        dags = {}
        dags.update(self._create_groupby_dag(mode, dags))
        dags.update(self._create_join_dag(mode, dags))

        return dags


config = list(yaml.safe_load_all(open(CONFIG_PATH, "r")))[0]

dag_constructor = DagConstructor(
    airflow_config=config.get("airflow_config"),
    airflow_dag_config=config.get("dag_config"),
    feature_config=config.get("feature_config"),
    operators_config=config.get("operators_config")
)

dags = dag_constructor.generate_dag()

g = globals()
g.update(dags)
