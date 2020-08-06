import yaml


class Config():
    def __init__(self):
        self.conf = None

    def set_config(self):
        if self.conf is None:
            self.conf = self.get_config()

    def get_config(self):
        try:
            with open('.exctxv2.yaml') as conf_file:
                conf = yaml.load(conf_file, Loader=yaml.FullLoader)
                return self.set_defaults(conf)
        except Exception as e:
            return self.set_defaults({})

    def set_defaults(self, conf):
        if conf.get('cicdtemplconfv2'):
            conf2 = conf['cicdtemplconfv2']
        else:
            conf['cicdtemplconfv2'] = {}
            conf2 = conf['cicdtemplconfv2']

        if conf2.get('clusters') is None:
            conf2['clusters'] = {}
        if conf2.get('exctxs') is None:
            conf2['exctxs'] = {}
        return conf2

    def store_conf(self):
        conf_to_write = {}
        conf_to_write['cicdtemplconfv2'] = self.conf
        try:
            with open('.exctxv2.yaml', 'w') as file:
                documents = yaml.dump(conf_to_write, file)
        except Exception as e:
            pass

    def get_existing_execution_context_id(self, cluster_id):
        self.set_config()
        return self.conf['exctxs'].get(cluster_id)

    def get_cluster_id(self, pipeline_folder, pipeline_name, any=False):
        self.set_config()
        pipeline_key = pipeline_folder + '_' + pipeline_name

        cluster_id = self.conf['clusters'].get(pipeline_key)
        if cluster_id is not None:
            return cluster_id
        elif any:
            _, cluster_id = next(iter( self.conf['clusters'].items() ))
            return cluster_id

    def store_cluster_for_pipeline(self, pipeline_folder, pipeline_name, cluster_id):
        self.set_config()
        pipeline_key = pipeline_folder + '_' + pipeline_name
        self.conf['clusters'][pipeline_key] = cluster_id
        self.store_conf()

    def store_execution_context_id(self, cluster_id, execution_context_id):
        self.conf = self.get_config()
        self.conf['exctxs'][cluster_id] = execution_context_id
        self.store_conf()
