import json
from json import JSONDecodeError
import os.path
from os import path
import yaml
import time

from mlflow.tracking.client import MlflowClient

import mlflow.sklearn
from os import listdir
from os.path import isfile, join, isdir, splitext

PIPELINE_RUNNER = 'pipeline_runner.py'


def wait_for_job_to_finish(client, run_id):
    while True:
        json_res = client.perform_query(method='GET', path='/jobs/runs/get-output', data=run_id)
        print(json_res)
        if json_res['metadata']['state']['life_cycle_state'] in ['TERMINATED', 'INTERNAL_ERROR']:
            return json_res['metadata']['state']['result_state']
        else:
            time.sleep(60)


def adjust_job_spec(job_spec, run_id, artifact_uri, libraries, version):
    job_spec['spark_python_task']['python_file'] = artifact_uri + '/' + PIPELINE_RUNNER
    job_spec['libraries'] = libraries
    # job_spec["custom_tags"] = [{"key": "run_uuid", "value": mlflow_run_uuid},
    #                            {"key": "mlflow_deployments_version", "value": version}]
    return job_spec


# def generate_job_spec(artifact_uri, libraries, path='deployment/job_template_aws.json'):
#     with open(path) as json_file:
#         job_spec = json.load(json_file)
#         job_spec = adjust_job_spec(job_spec, artifact_uri, libraries)
#         return job_spec


def prepare_libraries():
    # prepare libraries
    with open('runtime_requirements.txt') as file:
        libraries = [{'pypi': {'package': line.strip()}} for line in file if line.strip()]
        return libraries


def log_artifacts(model_name, libraries):
    job_files = ['runtime_requirements.txt']
    # log everything we need to mlflow
    with mlflow.start_run() as run:
        for f in job_files:
            if not os.path.isfile(f):
                raise FileNotFoundError(f"Please ensure `{f}` exists.")
            else:
                mlflow.log_artifact(f, artifact_path='job')

        mlflow.sklearn.log_model({"my dummy model"}, model_name, registered_model_name=model_name)

        if path.exists('dev-tests'):
            mlflow.log_artifact('dev-tests', artifact_path='job')
        if path.exists('integration-tests'):
            mlflow.log_artifact('integration-tests', artifact_path='job')
        if path.exists('pipelines'):
            mlflow.log_artifact('pipelines', artifact_path='job')

        for file in listdir('dist'):
            fullfile = join('dist', file)
            if isfile(fullfile):
                _, ext = splitext(fullfile)
                if ext.lower() in ['.whl', '.egg']:
                    mlflow.log_artifact(fullfile, artifact_path='dist')
                    libraries.append({ext[1:]: run.info._artifact_uri + '/dist/' + file})

        run_id = run.info.run_uuid
        artifact_uri = run.info._artifact_uri

        # job_spec = generate_job_spec(artifact_uri, libraries)
    print(run_id)
    print(artifact_uri)
    return run_id, artifact_uri


def read_config():
    # TODO: hard coding config_keys seems bad. Can I come up with something cleaner?
    config_keys = ['model-name', 'experiment-path', 'cloud']
    try:
        with open('deployment.yaml') as conf_file:
            conf = yaml.load(conf_file, Loader=yaml.FullLoader)
            for key in config_keys:
                try:
                    conf[key]
                except TypeError as e:
                    raise TypeError(
                        f"{e}. The deployment.yaml file in your root directory must contain a non-empty value for key: `{key}`")
                except KeyError as e:
                    raise KeyError(
                        f"{e}. `{key}` is not a valid key in your root level deployment.yaml file. The following is a list of valid keys: \n {config_keys}")
            model_name = conf['model-name']
            exp_path = conf['experiment-path']
            cloud = conf['cloud'].lower()
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"{e}. Please include a deployment.yaml file containing the following keys in your root directory:\n {config_keys}")
    print(model_name)
    print(exp_path)
    print(cloud)
    return model_name, exp_path, cloud


def submit_test_job(client, job_spec):
    job_run_id = client.perform_query(method='POST', path='/jobs/runs/submit', data=job_spec)
    print(job_run_id)
    res = wait_for_job_to_finish(client, job_run_id)
    print(res)
    return res


def check_if_dir_is_pipeline_def(dir, cloud):
    try:
        with open(join(dir, PIPELINE_RUNNER)):
            pass
    except FileNotFoundError as e:
        print('pipeline is expected to have a python script')
        return None
    try:
        with open(join(dir, 'job_spec_' + cloud + '.json')) as file:
            job_spec = json.load(file)
            return job_spec
    except FileNotFoundError as e:
        print('pipeline is expected to hava Databricks Job Definition in ')
    except JSONDecodeError as e:
        print('pipeline is expected to hava Databricks Job Definition in ')
    return None


def submit_one_job(client, dir, job_spec, run_id, artifact_uri, libraries, version):
    adjust_job_spec(job_spec, run_id, artifact_uri + '/job/' + dir, libraries, version)
    job_run_id = client.perform_query(method='POST', path='/jobs/runs/submit', data=job_spec)
    print(job_run_id)
    return job_run_id


def check_if_job_is_done(client, handle):
    json_res = client.perform_query(method='GET', path='/jobs/runs/get-output', data=handle)
    print(json_res)
    if json_res['metadata']['state']['life_cycle_state'] in ['TERMINATED', 'INTERNAL_ERROR']:
        return json_res['metadata']['state']['result_state']
    else:
        return None


def submit_jobs(client, root_folder, run_id, artifact_uri, libraries, cloud, version):
    submitted_jobs = []
    for file in listdir(root_folder):
        pipeline_path = join(root_folder, file)
        if isdir(pipeline_path):
            job_spec = check_if_dir_is_pipeline_def(pipeline_path, cloud)
            if job_spec is not None:
                submitted_job = submit_one_job(client, pipeline_path, job_spec, run_id, artifact_uri, libraries, version)
                if 'run_id' in submitted_job:
                    submitted_jobs.append(submitted_job)
                else:
                    print('Error while submitting job!')
                    return False

    while True:
        succesfull_jobs = []
        for handle in submitted_jobs:
            res = check_if_job_is_done(client, handle)
            if res is not None:
                if res == 'SUCCESS':
                    succesfull_jobs.append(handle)
                else:
                    print('Job ' + str(handle['run_id']) + ' was not successful.')
                    return False
        submitted_jobs = [x for x in submitted_jobs if x not in succesfull_jobs]
        if len(submitted_jobs) == 0:
            return True
        time.sleep(60)

def create_production_jobs(client, root_folder, run_id, artifact_uri, libraries, cloud, version):
    for file in listdir(root_folder):
        pipeline_path = join(root_folder, file)
        if isdir(pipeline_path):
            job_spec = check_if_dir_is_pipeline_def(pipeline_path, cloud)
            if job_spec is not None:
                adjust_job_spec(job_spec, run_id, artifact_uri + '/job/' + pipeline_path, libraries, version)
                print(job_spec)
                res = client.perform_query(method='POST', path='/jobs/create', data=job_spec)
                print(res)
                MlflowClient().set_tag(run_id, 'job_id', res)


def update_job_spec_for_prod_jobs(client, root_folder, run_id, artifact_uri, libraries, cloud, version):
    for file in listdir(root_folder):
        pipeline_path = join(root_folder, file)
        if isdir(pipeline_path):
            job_spec = check_if_dir_is_pipeline_def(pipeline_path, cloud)
            if job_spec is not None:
                adjust_job_spec(job_spec["new_settings"], run_id, artifact_uri + '/job/' + pipeline_path, libraries, version)
                print(job_spec)
                res = client.perform_query(method='POST', path='/jobs/reset', data=job_spec)
                print(res)
