import yaml


def get_existing_execution_context_id(cluster_id):
    try:
        with open('.exctx.yaml') as conf_file:
            conf = yaml.load(conf_file, Loader=yaml.FullLoader)
            exctx = conf[cluster_id]
            return exctx
    except Exception as e:
        return None


def store_execution_context_id(cluster_id, execution_context_id):
    try:
        conf = {}
        with open('.exctx.yaml', 'r') as conf_file:
            conf = yaml.load(conf_file, Loader=yaml.FullLoader)
    except Exception as e:
        pass
    conf[cluster_id] = execution_context_id
    try:
        with open('.exctx.yaml', 'w') as file:
            documents = yaml.dump(conf, file)
    except Exception as e:
        pass
