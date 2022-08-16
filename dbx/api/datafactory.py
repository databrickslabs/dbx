from pathlib import Path
from typing import List, Dict, Any, Union

from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    AzureDatabricksLinkedService,
    SecureString,
    LinkedServiceResource,
    DatabricksSparkPythonActivity,
    LinkedServiceReference,
    DatabricksSparkJarActivity,
    Activity,
    PipelineResource,
)
from azure.mgmt.subscription import SubscriptionClient
from databricks_cli.configure.provider import DatabricksConfig

from dbx.api.auth import AuthConfigProvider
from dbx.utils import dbx_echo
from dbx.utils.common import get_environment_data, transfer_profile_name
from dbx.utils.json import JsonUtils


class DatafactoryReflector:
    def __init__(
        self,
        specs_file: Path,
        subscription_name: str,
        resource_group: str,
        factory_name: str,
        name: str,
        environment: str,
    ):
        self.resource_group = resource_group
        self.factory_name = factory_name
        self.name = name
        self.environment = environment

        self.credential = DefaultAzureCredential(
            exclude_shared_token_cache_credential=True, exclude_visual_studio_code_credential=True
        )

        self.sub_client = SubscriptionClient(self.credential)
        self.subscription_id = self._get_subscription_id(subscription_name)
        self.adf_client = DataFactoryManagementClient(self.credential, subscription_id=self.subscription_id)

        self._specs = self._read_specs(specs_file, environment)
        self._config = self._get_config()

        self._verify_adf_setup()

    def _get_config(self) -> DatabricksConfig:
        environment_data = get_environment_data(self.environment)
        transfer_profile_name(environment_data)
        config = AuthConfigProvider.get_config()
        return config

    @staticmethod
    def _read_specs(specs_file: Path, environment: str) -> List[Dict[str, Any]]:
        specs = JsonUtils.read(specs_file).get(environment)

        if not specs:
            raise Exception(f"Environment {environment} not found in specs file {specs_file}")

        return specs.get("jobs", [])

    def _verify_adf_setup(self):
        # will raise not found error if factory or pipeline is non-existent
        try:
            self.adf_client.factories.get(resource_group_name=self.resource_group, factory_name=self.factory_name)
        except ResourceNotFoundError:
            raise ResourceNotFoundError(
                f"Factory with name {self.factory_name} not found in " f"resource group {self.resource_group}"
            )

        try:
            self.adf_client.pipelines.get(
                resource_group_name=self.resource_group, factory_name=self.factory_name, pipeline_name=self.name
            )
        except ResourceNotFoundError:
            ex_pipelines = self.adf_client.pipelines.list_by_factory(self.resource_group, self.factory_name)
            ex_pipeline_names = [p.name for p in ex_pipelines]
            raise ResourceNotFoundError(
                f"Pipeline {self.name} not found in factory {self.factory_name}."
                f"Existing pipeline names: {ex_pipeline_names}"
            )

    def _get_subscription_id(self, subscription_name: str) -> str:
        matched_subscriptions = [
            sub for sub in self.sub_client.subscriptions.list() if sub.display_name == subscription_name
        ]
        dbx_echo("Subscription list prepared")
        if not matched_subscriptions:
            raise Exception(f"Subscription with name {subscription_name} was not found")

        raw_subscription_id: str = matched_subscriptions[0].as_dict().get("id")
        subscription_id = raw_subscription_id.replace("/subscriptions/", "")
        return subscription_id

    def _create_linked_service(self, job_spec: Dict[str, Any]) -> str:
        cluster_spec = job_spec.get("new_cluster")
        service_name = job_spec.get("name") + "-ls"
        dbx_echo(f"Preparing linked service {service_name}")
        if cluster_spec:
            service_spec = AzureDatabricksLinkedService(
                domain=self._config.host,
                access_token=SecureString(value=self._config.token),
                instance_pool_id=cluster_spec.get("instance_pool_id"),
                new_cluster_custom_tags=cluster_spec.get("custom_tags"),
                new_cluster_driver_node_type=cluster_spec.get("driver_node_type_id"),
                new_cluster_enable_elastic_disk=cluster_spec.get("enable_elastic_disk"),
                new_cluster_init_scripts=cluster_spec.get("enable_elastic_disk"),
                new_cluster_log_destination=cluster_spec.get("cluster_log_conf", {}).get("dbfs", {}).get("destination"),
                new_cluster_node_type=cluster_spec.get("node_type_id"),
                new_cluster_num_of_worker=cluster_spec.get("num_workers"),
                new_cluster_spark_conf=cluster_spec.get("spark_conf"),
                new_cluster_spark_env_vars=cluster_spec.get("spark_env_vars"),
                new_cluster_version=cluster_spec.get("spark_version"),
            )
        else:
            service_spec = AzureDatabricksLinkedService(
                domain=self._config.host,
                access_token=SecureString(value=self._config.token),
                existing_cluster_id=job_spec.get("existing_cluster_id"),
            )

        service_resource = LinkedServiceResource(properties=service_spec)
        self.adf_client.linked_services.create_or_update(
            self.resource_group, self.factory_name, service_name, service_resource
        )

        dbx_echo(f"Preparing linked service {service_name} - done")
        return service_name

    @staticmethod
    def _generate_python_activity(job_spec: Dict[str, Any], service_name) -> DatabricksSparkPythonActivity:
        activity = DatabricksSparkPythonActivity(
            name=job_spec.get("name"),
            linked_service_name=LinkedServiceReference(reference_name=service_name),
            python_file=job_spec.get("spark_python_task").get("python_file"),
            parameters=job_spec.get("spark_python_task").get("parameters", []),
            libraries=job_spec.get("libraries", []),
        )
        return activity

    @staticmethod
    def _generate_spark_jar_task_activity(job_spec: Dict[str, Any], service_name) -> DatabricksSparkJarActivity:
        activity = DatabricksSparkJarActivity(
            name=job_spec.get("name"),
            linked_service_name=LinkedServiceReference(reference_name=service_name),
            main_class_name=job_spec.get("spark_jar_task").get("main_class_name"),
            parameters=job_spec.get("spark_jar_task").get("parameters", []),
            libraries=job_spec.get("libraries", []),
        )
        return activity

    def _update_pipeline(self, new_activities: List[Union[DatabricksSparkPythonActivity, DatabricksSparkJarActivity]]):
        dbx_echo(f"Updating pipeline {self.name}")
        current_pipeline = self.adf_client.pipelines.get(self.resource_group, self.factory_name, self.name)

        new_activities_dict = {a.name: a for a in new_activities}
        existing_activities_dict = {a.name: a for a in current_pipeline.activities}
        intersected = set(new_activities_dict.keys()).intersection(set(existing_activities_dict.keys()))

        final_activity_list = []

        for name in intersected:
            _ex: Activity = existing_activities_dict.get(name)
            _new: Activity = new_activities_dict.get(name)
            _new.depends_on = _ex.depends_on
            final_activity_list.append(_new)

        for name, activity in existing_activities_dict.items():
            if name not in intersected:
                final_activity_list.append(activity)

        for name, activity in new_activities_dict.items():
            if name not in intersected:
                final_activity_list.append(activity)

        resource = PipelineResource(
            activities=final_activity_list,
            description=current_pipeline.description,
            parameters=current_pipeline.parameters,
            variables=current_pipeline.variables,
            concurrency=current_pipeline.concurrency,
            annotations=current_pipeline.annotations,
            run_dimensions=current_pipeline.run_dimensions,
            folder=current_pipeline.folder,
            policy=current_pipeline.policy,
        )

        self.adf_client.pipelines.create_or_update(self.resource_group, self.factory_name, self.name, resource)
        dbx_echo(f"Updating pipeline {self.name} - done")

    def launch(self):
        dbx_echo(f"Starting deployment to factory {self.factory_name}")
        prepared_activities = []
        for job_spec in self._specs:
            job_name = job_spec.get("name")
            dbx_echo(f"Preparing job {job_name}")
            service_name = self._create_linked_service(job_spec)

            if job_spec.get("spark_python_task"):
                job_activity = self._generate_python_activity(job_spec, service_name)
            elif job_spec.get("spark_jar_task"):
                job_activity = self._generate_spark_jar_task_activity(job_spec, service_name)
            else:
                raise Exception(
                    "Neither spark_python_task nor spark_jar_task were provided in the job definition."
                    "Please add one of them, or create an issue to support another type"
                )

            prepared_activities.append(job_activity)
            dbx_echo(f"Preparing job {job_name} - done")
        self._update_pipeline(prepared_activities)
        dbx_echo(f"Reflection to factory {self.factory_name} finished successfully")