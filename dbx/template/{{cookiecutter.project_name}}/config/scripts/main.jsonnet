local environments = ["test", "live"];
local jobs = ["batch"];

local providers = import 'providers.libsonnet';

local deployments = {
    ["deployments/%s-%s.json" % [env, job]]: {
            "new_cluster": providers.cluster(env, job),
            "name": providers.name(env, job),
            "max_retries": 0,
            "spark_python_task": {
                    "python_file": providers.pythonFile(env, job),
                    "parameters": providers.jobArguments(env, job)
                }
        }
    for env in environments
    for job in jobs
};

local parameters = {
    ["parameters/%s-%s.json" % [env, job]]: providers.parameters(env, job)
    for env in environments
    for job in jobs
};

local files = deployments + parameters;

files