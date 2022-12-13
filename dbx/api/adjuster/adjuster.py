from typing import Any, Optional, Union, List

from databricks_cli.sdk import ApiClient
from pydantic import BaseModel

from dbx.api.adjuster.mixins.existing_cluster import ExistingClusterAdjuster
from dbx.api.adjuster.mixins.file_reference import FileReferenceAdjuster
from dbx.api.adjuster.mixins.instance_pool import InstancePoolAdjuster
from dbx.api.adjuster.mixins.instance_profile import InstanceProfileAdjuster
from dbx.api.adjuster.mixins.pipeline import PipelineAdjuster
from dbx.api.adjuster.mixins.service_principal import ServicePrincipalAdjuster
from dbx.api.adjuster.mixins.sql_properties import SqlPropertiesAdjuster
from dbx.api.adjuster.policy import PolicyAdjuster
from dbx.models.deployment import WorkflowList
from dbx.models.workflow.common.flexible import FlexibleModel
from dbx.models.workflow.common.libraries import Library
from dbx.models.workflow.common.new_cluster import NewCluster
from dbx.models.workflow.v2dot0.workflow import Workflow as V2dot0Workflow
from dbx.models.workflow.v2dot1.job_cluster import JobCluster
from dbx.models.workflow.v2dot1.job_task_settings import JobTaskSettings
from dbx.utils import dbx_echo
from dbx.utils.file_uploader import AbstractFileUploader


class AdditionalLibrariesProvider(FlexibleModel):
    no_package: Optional[bool] = False
    core_package: Optional[Library]
    libraries_from_requirements: Optional[List[Library]] = []


class PropertyAdjuster(
    InstancePoolAdjuster,
    ExistingClusterAdjuster,
    InstanceProfileAdjuster,
    PipelineAdjuster,
    ServicePrincipalAdjuster,
    SqlPropertiesAdjuster,
    PolicyAdjuster,
):
    def traverse(self, _object: Any, parent: Optional[Any] = None, index_in_parent: Optional[Any] = None):

        # if element is a dictionary, simply continue traversing
        if isinstance(_object, dict):
            for key in list(_object.keys()):
                item = _object[key]
                yield item, _object, key
                for _out in self.traverse(item, _object, index_in_parent):
                    yield _out

        # if element is a list, simply continue traversing
        elif isinstance(_object, list):
            for idx, sub_item in enumerate(_object):
                yield sub_item, _object, idx
                for _out in self.traverse(sub_item, _object, idx):
                    yield _out

        # process any other kind of nested references
        elif isinstance(_object, (BaseModel, FlexibleModel)):
            for key, sub_element in _object.__dict__.items():
                if sub_element is not None:
                    yield sub_element, _object, key
                    for _out in self.traverse(sub_element, _object, key):
                        yield _out
        else:
            # yield the low-level objects
            yield _object, parent, index_in_parent

    @staticmethod
    def _preprocess_libraries(
        element: Union[JobTaskSettings, V2dot0Workflow], additional_libraries: AdditionalLibrariesProvider
    ):
        _element_string = (
            f"workflow {element.name}" if isinstance(element, V2dot0Workflow) else f"task {element.task_key}"
        )
        dbx_echo(f"Processing libraries for {_element_string}")
        element.libraries += additional_libraries.libraries_from_requirements
        if additional_libraries.no_package or (element.deployment_config and element.deployment_config.no_package):
            pass
        else:
            element.libraries += [additional_libraries.core_package] if additional_libraries.core_package else []
        dbx_echo(f"✅ Processing libraries for {_element_string} - done")

    def library_traverse(self, workflows: WorkflowList, additional_libraries: AdditionalLibrariesProvider):

        for element, _, __ in self.traverse(workflows):

            if isinstance(element, (V2dot0Workflow, JobTaskSettings)):
                self._preprocess_libraries(element, additional_libraries)

    def _new_cluster_handler(self, element: NewCluster):
        # driver_instance_pool_name -> driver_instance_pool_id
        if element.driver_instance_pool_name is not None:
            self._adjust_legacy_driver_instance_pool_ref(element)
        # instance_pool_name -> instance_pool_id
        if element.instance_pool_name is not None:
            self._adjust_legacy_instance_pool_ref(element)
        # instance_profile_name -> instance_profile_arn
        if element.aws_attributes is not None and element.aws_attributes.instance_profile_name is not None:
            self._adjust_legacy_instance_profile_ref(element)

    def property_traverse(self, workflows: WorkflowList):
        """
        This traverse applies all the transformations to the workflows
        :param workflows:
        :return: None
        """
        for element, parent, index in self.traverse(workflows):

            if isinstance(element, V2dot0Workflow):
                # legacy named conversion
                # existing_cluster_name -> existing_cluster_id
                if element.existing_cluster_name is not None:
                    self._adjust_legacy_existing_cluster(element)

            if isinstance(element, NewCluster):
                self._new_cluster_handler(element)

            if isinstance(element, str):

                if element.startswith("cluster://"):
                    self._adjust_existing_cluster_ref(element, parent, index)

                elif element.startswith("instance-profile://"):
                    self._adjust_instance_profile_ref(element, parent, index)

                elif element.startswith("instance-pool://"):
                    self._adjust_instance_pool_ref(element, parent, index)

                elif element.startswith("pipeline://"):
                    self._adjust_pipeline_ref(element, parent, index)

                elif element.startswith("service-principal://"):
                    self._adjust_service_principal_ref(element, parent, index)

                elif element.startswith("warehouse://"):
                    self._adjust_warehouse_ref(element, parent, index)

                elif element.startswith("query://"):
                    self._adjust_query_ref(element, parent, index)

                elif element.startswith("dashboard://"):
                    self._adjust_dashboard_ref(element, parent, index)

                elif element.startswith("alert://"):
                    self._adjust_alert_ref(element, parent, index)

    def cluster_policy_traverse(self, workflows: WorkflowList):
        """
        This traverse applies only the policy_name OR policy_id traverse.
        Please note that this traverse should go STRICTLY after all other rules,
        when ids and other transformations are already resolved.
        :param workflows:
        :return: None
        """
        for element, parent, _ in self.traverse(workflows):
            if isinstance(parent, (V2dot0Workflow, JobTaskSettings, JobCluster)) and isinstance(element, NewCluster):
                if element.policy_name is not None or (
                    isinstance(element, NewCluster)
                    and element.policy_id is not None
                    and element.policy_id.startswith("cluster-policy://")
                ):
                    element = self._adjust_policy_ref(element)
                parent.new_cluster = element

    def file_traverse(self, workflows, file_adjuster: FileReferenceAdjuster):
        for element, parent, index in self.traverse(workflows):
            if isinstance(element, str):
                if element.startswith("file://") or element.startswith("file:fuse://"):
                    file_adjuster.adjust_file_ref(element, parent, index)


class Adjuster:
    def __init__(
        self,
        additional_libraries: AdditionalLibrariesProvider,
        file_uploader: AbstractFileUploader,
        api_client: ApiClient,
    ):
        self.property_adjuster = PropertyAdjuster(api_client=api_client)
        self.file_adjuster = FileReferenceAdjuster(file_uploader)
        self.additional_libraries = additional_libraries

    def traverse(self, workflows: Union[WorkflowList, List[str]]):
        dbx_echo("Starting the traversal process")
        self.property_adjuster.library_traverse(workflows, self.additional_libraries)
        self.property_adjuster.file_traverse(workflows, self.file_adjuster)
        self.property_adjuster.property_traverse(workflows)
        self.property_adjuster.cluster_policy_traverse(workflows)
        dbx_echo("Traversal process finished, all provided references were resolved")
