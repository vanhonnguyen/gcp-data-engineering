#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Example Airflow DAG for Google Cloud Dataflow service
"""
import os
from typing import Callable, Dict, List
from urllib.parse import urlparse

from airflow import models
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.operators.dataflow import (
    CheckJobRunning,
    DataflowCreateJavaJobOperator,
    DataflowCreatePythonJobOperator,
    DataflowTemplatedJobStartOperator,
)
from airflow.providers.google.cloud.sensors.dataflow import (
    DataflowJobAutoScalingEventsSensor,
    DataflowJobMessagesSensor,
    DataflowJobMetricsSensor,
    DataflowJobStatusSensor,
)
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.utils.dates import days_ago

"""
GCS_TMP = os.environ.get('GCP_DATAFLOW_GCS_TMP', 'gs://test-dataflow-example/temp/')
GCS_STAGING = os.environ.get('GCP_DATAFLOW_GCS_STAGING', 'gs://test-dataflow-example/staging/')
GCS_OUTPUT = os.environ.get('GCP_DATAFLOW_GCS_OUTPUT', 'gs://test-dataflow-example/output')
GCS_PYTHON = os.environ.get('GCP_DATAFLOW_PYTHON', 'gs://us-central1-composer-advanc-cb2156e7-bucket/dags/data_ingestion.py')

bucket_path = models.Variable.get("bucket_path")
project_id = models.Variable.get("project_id")
gce_zone = models.Variable.get("gce_zone")
gce_region = models.Variable.get("gce_region")

"""

default_args = {
    'dataflow_default_options': {
        'project': 'project-vn-test-composer',
        'region': 'us-central1',
        'stagingLocation': 'gs://us-central1-composer-advanc-cb2156e7-bucket/test/',
        'tempLocation': 'gs://us-central1-composer-advanc-cb2156e7-bucket/temp/',
    }
}

with models.DAG(
    "example_dag_wordcount",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # Override to match your needs
    tags=['example'],
) as dag_native_python:

    # [START howto_operator_start_python_job]
    start_python_job = DataflowCreatePythonJobOperator(
        task_id="start-python-job",
        py_file='gs://us-central1-composer-advanc-cb2156e7-bucket/dags/dag_wordcount.py',
        py_options=[],
        job_name='{{task.task_id}}',
        options={
            'input': 'gs://dataflow-samples/shakespeare/kinglear.txt',
            'output': 'gs://us-central1-composer-advanc-cb2156e7-bucket/results/outputs',
        },
        py_requirements=['apache-beam[gcp]==2.24.0'],
        py_interpreter='python3',
        py_system_site_packages=False,
        location='us-central1',
    )
    # [END howto_operator_start_python_job]

