import json
from json import JSONDecodeError
import os.path
from os import path
import yaml
import time
import pkg_resources

from mlflow.tracking.client import MlflowClient

import mlflow.sklearn
from os import listdir
from os.path import isfile, join, isdir, splitext

from databricks_cli.configure.provider import get_config
from databricks_cli.configure.config import _get_api_client

PIPELINE_RUNNER = 'pipeline_runner.py'
PACKAGE_NAME = 'databrickslabs_cicdtemplates'
PRD_NAME = 'cicdtemplates-'


def set_mlflow_experiment_path(exp_path):
    try:
        mlflow.set_experiment(exp_path)
    except Exception as e:
        raise Exception(f"""{e}.
        Have you added the following secrets to your github repo?
            secrets.DATABRICKS_HOST
            secrets.DATABRICKS_TOKEN""")


def getDatabricksAPIClient():
    try:
        version = pkg_resources.get_distribution(PACKAGE_NAME).version
        apiClient = _get_api_client(get_config(), command_name=PRD_NAME + version)
        return apiClient
    except Exception as e:
        raise Exception(f"""{e}.
        Have you added the following secrets to your github repo?
            secrets.DATABRICKS_HOST
            secrets.DATABRICKS_TOKEN""")


def wait_for_job_to_finish(client, run_id):
    while True:
        json_res = client.perform_query(method='GET', path='/jobs/runs/get-output', data=run_id)
        print(json_res)
        if json_res['metadata']['state']['life_cycle_state'] in ['TERMINATED', 'INTERNAL_ERROR']:
            return json_res['metadata']['state']['result_state']
        else:
            time.sleep(60)


def wait_for_cluster_to_start(client, cluster_id):
    while True:
        json_res = client.perform_query(method='GET', path='/clusters/get', data={'cluster_id': cluster_id})
        print(json_res['state'])
        if json_res['state'] in ['TERMINATED', 'ERROR', 'UNKNOWN']:
            return 'TERMINATED'
        elif json_res['state'] in ['RUNNING']:
            time.sleep(30)
            return 'RUNNING'
        else:
            time.sleep(60)


def adjust_job_spec(job_spec, artifact_uri, pipeline_root, libraries):
    task_node = job_spec['spark_python_task']
    task_node['python_file'] = artifact_uri + '/' + PIPELINE_RUNNER
    if task_node.get('parameters'):
        params = task_node['parameters']
        task_node['parameters'] = [artifact_uri] + params
    else:
        task_node['parameters'] = [artifact_uri]
    libraries = libraries + gen_pipeline_dependencies(pipeline_root + '/dependencies', artifact_uri + '/dependencies')
    if job_spec.get('libraries'):
        job_spec['libraries'] = job_spec['libraries'] + libraries
    else:
        job_spec['libraries'] = libraries
    return job_spec


def prepare_libraries():
    # prepare libraries
    with open('runtime_requirements.txt') as file:
        libraries = [{'pypi': {'package': line.strip()}} for line in file if line.strip()]
        return libraries


def log_artifacts(model_name, libraries, register_model=False, dirs_to_deploy=None):
    if dirs_to_deploy is None:
        dirs_to_deploy = ['dev-tests', 'integration-tests', 'pipelines', 'dependencies']
    job_files = ['runtime_requirements.txt']
    model_version = None
    # log everything we need to mlflow
    with mlflow.start_run() as run:
        for f in job_files:
            if not os.path.isfile(f):
                raise FileNotFoundError(f"Please ensure `{f}` exists.")
            else:
                mlflow.log_artifact(f, artifact_path='job')

        if register_model:
            mlflow.sklearn.log_model({"my dummy model"}, model_name)
            try:
                model_version = mlflow.register_model("runs:/" + run.info.run_uuid + "/" + model_name, model_name)
            except Exception as e:
                print(e)
                print('Error registering model version. It looks like Model Registry is not available.')
                model_version = None

        for dir in dirs_to_deploy:
            if path.exists(dir):
                mlflow.log_artifact(dir, artifact_path='job')

        current_artifacts = []
        for file in listdir('dist'):
            fullfile = join('dist', file)
            if isfile(fullfile):
                _, ext = splitext(fullfile)
                if ext.lower() in ['.whl', '.egg']:
                    mlflow.log_artifact(fullfile, artifact_path='dist')
                    dist_file = run.info._artifact_uri + '/dist/' + file
                    current_artifacts.append(dist_file)
                    if libraries:
                        libraries.append({ext[1:]: dist_file})
        if libraries:
            libraries = libraries + gen_pipeline_dependencies('dependencies',
                                                              run.info._artifact_uri + '/job/dependencies')

        run_id = run.info.run_uuid
        artifact_uri = run.info._artifact_uri

    print(run_id)
    print(artifact_uri)
    return run_id, artifact_uri, model_version, libraries, current_artifacts


def gen_pipeline_dependencies(root_folder, artifact_uri):
    res = []
    if path.exists(root_folder + '/jars'):
        for file in listdir(root_folder + '/jars'):
            res.append({'jar': artifact_uri + '/jars/' + file})
    if path.exists(root_folder + '/wheels'):
        for file in listdir(root_folder + '/wheels'):
            _, ext = splitext(root_folder + '/wheels/' + file)
            if ext.lower() in ['.whl', '.egg']:
                res.append({ext[1:]: artifact_uri + '/wheels/' + file})
    return res


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


def check_if_dir_is_pipeline_def(dir, cloud, env):
    try:
        with open(join(dir, PIPELINE_RUNNER)):
            pass
    except FileNotFoundError as e:
        print('Pipeline is expected to have a python script')
        return None
    try:
        conf_path = join(dir, 'job_spec_' + cloud + '.json')
        if env is not None:
            conf_path_env = join(dir, 'job_spec_' + cloud + '_' + env + '.json')
            if path.exists(conf_path_env):
                conf_path = conf_path_env

        with open(conf_path) as file:
            job_spec = json.load(file)
            return job_spec
    except FileNotFoundError as e:
        print('Pipeline is expected to hava Databricks Job Definition in ', conf_path)
    except JSONDecodeError as e:
        print('Pipeline is expected to hava Databricks Job Definition in ',conf_path)
    return None


def submit_one_job(client, dir, job_spec, artifact_uri, libraries):
    adjust_job_spec(job_spec, artifact_uri + '/job/' + dir, dir, libraries)
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


def submit_jobs_for_one_pipeline(client, pipeline_path, artifact_uri, libraries, cloud, env):
    job_spec = check_if_dir_is_pipeline_def(pipeline_path, cloud, env)
    if job_spec is not None:
        submitted_job = submit_one_job(client, pipeline_path, job_spec, artifact_uri, libraries)
        if 'run_id' in submitted_job:
            return submitted_job
        else:
            print('Error while submitting job!')
            return None


def submit_jobs_for_all_pipelines(client, root_folder, artifact_uri, libraries, cloud, env, pipeline_name=None):
    submitted_jobs = []
    for file in listdir(root_folder):
        if (not pipeline_name) or (pipeline_name and file == pipeline_name):
            pipeline_path = join(root_folder, file)
            if isdir(pipeline_path):
                submitted_job = submit_jobs_for_one_pipeline(client, pipeline_path, artifact_uri, libraries, cloud, env)
                if submitted_job:
                    submitted_jobs.append(submitted_job)

    return wait_for_all_jobs_to_finish(client, submitted_jobs)


def wait_for_all_jobs_to_finish(client, submitted_jobs):
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


def get_existing_job_ids_for_selected_run(run_id):
    res = {}
    try:
        run = MlflowClient().get_run(run_id)
        for tag in run.data.tags:
            if '_job_id' in tag:
                value = run.data.tags[tag]
                res[tag.replace('_job_id', '')] = value
        return res
    except Exception as e:
        print(e)
        return {}


def get_existing_job_ids(model_name, stages):
    try:
        versions = MlflowClient().get_latest_versions(name=model_name, stages=stages)
        if versions:
            for version in versions:
                job_ids = get_existing_job_ids_for_selected_run(version.run_id)
                if job_ids:
                    return job_ids
        print("No older versions found")
    except Exception as e:
        print(e)
        print('Error has occured while determining previous job versions.')
    return dict()


def check_if_job_exists(client, job_id):
    try:
        res = client.perform_query(method='GET', path='/jobs/get', data={'job_id': job_id})
        print(res)
        return True
    except:
        return False


def create_or_update_production_jobs(client, root_folder, run_id, artifact_uri, libraries, cloud, env, model_name,
                                     stages, model_version):
    job_ids = get_existing_job_ids(model_name, stages)
    for file in listdir(root_folder):
        pipeline_path = join(root_folder, file)
        pipeline_name = file.lower()
        if isdir(pipeline_path):
            job_spec = check_if_dir_is_pipeline_def(pipeline_path, cloud, env)
            if job_spec is not None:
                if job_ids.get(pipeline_name):
                    job_id = job_ids[pipeline_name]
                    print('Found deployed job with ID ', job_id,
                          '. Checking if the job with this ID is registered in Databricks workspace...')
                    if check_if_job_exists(client, job_id):
                        print('Production job with ID ', job_id, ' exists. Updating job definition...')
                        update_production_job(client, job_id, run_id, artifact_uri, pipeline_name, pipeline_path,
                                              libraries, job_spec)
                    else:
                        print('Production job with ID ', job_id, ' does not exist. Creating new one...')
                        create_production_job(client, run_id, artifact_uri, pipeline_name, pipeline_path, libraries,
                                              job_spec)
                else:
                    # existing job for the current pipeline does not exists - creating new
                    print('Existing job for the pipeline with name [', pipeline_name, '] does not exists')
                    print('Creating new one...')
                    create_production_job(client, run_id, artifact_uri, pipeline_name, pipeline_path, libraries,
                                          job_spec)
    try:
        MlflowClient().transition_model_version_stage(name=model_name, version=model_version.version,
                                                      stage="production")
    except:
        print('Error transitioning model version. It looks like Model Registry is not available.')


def create_production_job(client, run_id, artifact_uri, pipeline_name, pipeline_path, libraries, job_spec):
    adjust_job_spec(job_spec, artifact_uri + '/job/' + pipeline_path, pipeline_path, libraries)
    print(job_spec)
    res = client.perform_query(method='POST', path='/jobs/create', data=job_spec)
    print('Created job with ID ', res['job_id'])
    MlflowClient().set_tag(run_id, pipeline_name + '_job_id', res['job_id'])
    return res


def update_production_job(client, job_id, run_id, artifact_uri, pipeline_name, pipeline_path, libraries, job_spec):
    adjust_job_spec(job_spec, artifact_uri + '/job/' + pipeline_path, pipeline_path, libraries)
    reset_job_spec = dict()
    reset_job_spec['job_id'] = job_id
    reset_job_spec['new_settings'] = job_spec
    print(reset_job_spec)
    res = client.perform_query(method='POST', path='/jobs/reset', data=reset_job_spec)
    print('Finished updating production job. Result: ')
    print(res)
    MlflowClient().set_tag(run_id, pipeline_name + '_job_id', job_id)


def create_cluster(client, job_spec, cluster_name=None):
    if job_spec.get('new_cluster'):
        cluster_spec = job_spec['new_cluster']
        if cluster_name:
            cluster_spec['cluster_name'] = cluster_name

        if cluster_spec.get('spark_conf'):
            cluster_spec['spark_conf']['spark.databricks.conda.condaMagic.enabled'] = 'true'
        else:
            cluster_spec['spark_conf'] = {'spark.databricks.conda.condaMagic.enabled': 'true'}

        res = client.perform_query(method='POST', path='/clusters/create', data=cluster_spec)
        if res and res.get('cluster_id'):
            return res['cluster_id']
        else:
            print('Error creating cluster: ', res)
            return None
    else:
        print('Cannot find cluster definition in job specification!')
        return None


def install_libraries(client, dir, pipeline_name, cloud, env, cluster_id, libraries, artifact_uri):
    pipeline_path = dir + '/' + pipeline_name
    job_spec = check_if_dir_is_pipeline_def(pipeline_path, cloud, env)
    if job_spec:
        libraries = libraries + gen_pipeline_dependencies(pipeline_path + '/dependencies',
                                                          artifact_uri + '/job/' + pipeline_path + '/dependencies')
        res = client.perform_query(method='POST', path='/libraries/install',
                                   data={'cluster_id': cluster_id, 'libraries': libraries})

        print(res)

    else:
        print('Cannot find pipeline ', pipeline_name, ' in directory ', dir, ' for the cloud ', cloud)


def generate_libraries_cell(libraries):
    code = ''
    for d in libraries:
        library = next(iter(d.values()))
        if not (type(library) == str):
            library = next(iter(library.values()))
        if not ('.jar' in library):
            library = library.replace('dbfs:/', '/dbfs/')
            code += (library + ' ')
    return '%pip install  ' + code


def generate_artifacts_cell(libraries):
    code = ''
    for library in libraries:
        if not ('.jar' in library):
            library = library.replace('dbfs:/', '/dbfs/')
            code += ('%pip install --upgrade ' + library + ' \n')
    return code


def wait_until(cmd, check_fn, timeout, period=5, *args, **kwargs):
    mustend = time.time() + timeout
    while time.time() < mustend:
        cmd_res = cmd(*args, **kwargs)
        if check_fn(cmd_res):
            return cmd_res
        time.sleep(period)
    return None


def wait_for_result_of_command(client, cluster_id, ctx_id, cmd_id):
    def check_command_status(cluster_id, ctx_id, cmd_id):
        try:
            return client.perform_query(method='GET', path='/commands/status',
                                        data={'clusterId': cluster_id, 'contextId': ctx_id, 'commandId': cmd_id})
        except Exception as e:
            print('Error has occurred: ', e)
            return None

    def is_finished(res):
        try:
            if res is not None:
                return res['status'] == 'Finished'
            else:
                return False
        except:
            return False

    res = wait_until(check_command_status, is_finished, 24 * 3600, 5, cluster_id, ctx_id, cmd_id)
    return res


def execute_command_sync(client, cluster_id, ctx_id, cmd_txt):
    cmd_id = None
    try:
        print('Sending command:')
        print(cmd_txt)
        res = client.perform_query(method='POST', path='/commands/execute',
                                   data={'language': 'python', 'clusterId': cluster_id, 'contextId': ctx_id,
                                         'command': cmd_txt})
        cmd_id = res['id']
        res = wait_for_result_of_command(client, cluster_id, ctx_id, cmd_id)
        print('Result:')
        try:
            if res['results']['resultType'] == 'error':
                print(res['results']['cause'])
            else:
                print(res['results']['data'])
        except:
            print(res)
        return res
    except KeyboardInterrupt as e:
        if cmd_id is not None:
            print('Interrupted. Stopping command execution...')
            client.perform_query(method='POST', path='/commands/cancel',
                                 data={'clusterId': cluster_id, 'contextId': ctx_id, 'commandId': cmd_id})
        raise Exception(e)
    except Exception as e:
        print('Error has occurred: ', e)
        return None


def submit_one_pipeline_to_exctx(client, artifact_uri, pipeline_dir, pipeline_name, libraries, current_artifacts, cloud,
                                 env,
                                 cluster_id, execution_context_id):
    client.url = client.url.replace('/api/2.0', '/api/1.2')
    pipeline_path = join(pipeline_dir, pipeline_name)
    job_spec = check_if_dir_is_pipeline_def(pipeline_path, cloud, env)
    if job_spec is not None:
        if libraries:
            lib_cell = generate_libraries_cell(libraries)
        else:
            lib_cell = generate_artifacts_cell(current_artifacts)
        # install libraries
        execute_command_sync(client, cluster_id, execution_context_id, lib_cell)
        # set param
        # params = ['', artifact_uri]
        # task_node = job_spec['spark_python_task']
        # if task_node.get('parameters'):
        #    params = task_node['parameters']
        # params = ['\''+p+'\'' for p in params]
        code = 'import sys\nsys.argv = [\'\', \'' + artifact_uri + '/job/' + pipeline_path + '\']'
        execute_command_sync(client, cluster_id, execution_context_id, code)

        # execute actual code
        with open(join(pipeline_path, PIPELINE_RUNNER), 'r') as content_file:
            content = content_file.read()
            ex_res = execute_command_sync(client, cluster_id, execution_context_id, content)


def ensure_exution_context_exists(client, cluster_id, ex_ctx_id):
    client.url = client.url.replace('/api/2.0', '/api/1.2')
    try:
        res = client.perform_query(method='GET', path='/contexts/status',
                                   data={'clusterId': cluster_id, 'contextId': ex_ctx_id})
        return True
    except:
        return False


def create_exution_context_exists(client, cluster_id):
    client.url = client.url.replace('/api/2.0', '/api/1.2')

    try:
        res = client.perform_query(method='POST', path='/contexts/create',
                                   data={'language': 'python', 'clusterId': cluster_id})
        ex_ctx_id = res['id']
        return ex_ctx_id
    except Exception as e:
        print('Error has occured while creating context: ', e)
        return None
