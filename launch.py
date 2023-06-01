#
# Copyright (C) 2023 Databricks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import click
from databricks_cli.sdk import ApiClient, JobsService
import logging
from pprint import pprint
import time

logging.basicConfig(
     format= '%(asctime)s %(name)s %(levelname)s - %(message)s',
     datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_api_client():
    """Retrieve an API client object using creds from the environment."""
    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]
    jobs_api_version = "2.1"
    api_client = ApiClient(
        host=host,
        token=token,
        jobs_api_version=jobs_api_version)
    return api_client


def format_task_update_message(task):
    """Format a single task state for logging / output."""
    message = f"""{task["task_key"]}: {task["state"]["life_cycle_state"]}"""
    if "result_state" in task["state"]:
        message = f"""{message}:{task["state"]["result_state"]}"""
    return message


def format_job_update_message(run_info):
    """Prepare the message we want to log for an entire job."""
    task_life_cycle_states = [
        format_task_update_message(task)
        for task in run_info["tasks"]
    ]
    message = ", ".join(task_life_cycle_states)
    return message


def wait_run(run_id):
    """Wait until the given run_id reaches a terminal state and then return it."""
    job_update_polling_interval_secs = 1
    api_client = get_api_client()
    jobs_service = JobsService(api_client)
    run_life_cycle_state = None
    terminal_run_life_cycle_states = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
    while True:
        run_info = jobs_service.get_run(run_id)
        run_life_cycle_state = run_info["state"]["life_cycle_state"]
        if run_life_cycle_state in terminal_run_life_cycle_states:
            return run_info
        else:
            update_message = format_job_update_message(run_info)
            logger.info(update_message)
        time.sleep(job_update_polling_interval_secs)


@click.command()
@click.option('--job-id', help='JOB-ID to run and track')
def launch(job_id: str):
    """Run the given job and wait until the run terminates."""
    api_client = get_api_client()
    jobs_service = JobsService(api_client)
    print(api_client)
    print(jobs_service)
    try:
        logger.info(f"launching job-id {job_id}")
        run_info = jobs_service.run_now(job_id)
        run_id = run_info["run_id"]
        logger.info("successfully launched job %d", run_id)
        final_state = wait_run(run_id)
        logger.info(final_state)
    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    launch()


