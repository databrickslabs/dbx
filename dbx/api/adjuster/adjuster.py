from typing import Any, Optional, Union, List

from pydantic import BaseModel

from dbx.api.adjuster._mixins import (
    InstancePoolAdjuster,
    ExistingClusterAdjuster,
    InstanceProfileAdjuster,
    FileReferenceAdjuster,
    PipelineAdjuster,
    ServicePrincipalAdjuster,
    WarehouseAdjuster,
    QueryAdjuster,
    DashboardAdjuster,
    AlertAdjuster,
)
from dbx.api.adjuster.policy import PolicyAdjuster
from dbx.models.deployment import WorkflowList
from dbx.models.workflow.common.flexible import FlexibleModel
from dbx.models.workflow.common.libraries import Library
from dbx.models.workflow.common.new_cluster import NewCluster
from dbx.models.workflow.v2dot0.workflow import Workflow as V2dot0Workflow
from dbx.models.workflow.v2dot1.job_task_settings import JobTaskSettings
from dbx.models.workflow.v2dot1.workflow import Workflow as V2dot1Workflow


class Adjuster(
    InstancePoolAdjuster,
    ExistingClusterAdjuster,
    InstanceProfileAdjuster,
    FileReferenceAdjuster,
    PipelineAdjuster,
    ServicePrincipalAdjuster,
    WarehouseAdjuster,
    QueryAdjuster,
    DashboardAdjuster,
    AlertAdjuster,
    PolicyAdjuster,
):
    def __init__(self, additional_libraries: List[Library], **kwargs):
        self.additional_libraries = additional_libraries
        super().__init__(**kwargs)

    def _traverse(self, _object: Any, parent: Optional[Any] = None, index_in_parent: Optional[Any] = None):

        # if element is a dictionary, simply continue traversing
        if isinstance(_object, dict):
            for key, item in _object.items():
                yield item, _object, key
                for _out in self._traverse(item, _object, index_in_parent):
                    yield _out

        # if element is a list, simply continue traversing
        elif isinstance(_object, list):
            for idx, sub_item in enumerate(_object):
                yield sub_item, _object, idx
                for _out in self._traverse(sub_item, _object, idx):
                    yield _out

        # process any other kind of nested references
        elif isinstance(_object, (BaseModel, FlexibleModel)):
            for key, sub_element in _object.__dict__.items():
                if sub_element is not None:
                    yield sub_element, _object, key
                    for _out in self._traverse(sub_element, _object, key):
                        yield _out
        else:
            # yield the low-level objects
            yield _object, parent, index_in_parent

    def _library_traverse(self, workflows: WorkflowList):

        for element, parent, index in self._traverse(workflows):
            print(element, parent, index)
            if isinstance(element, V2dot1Workflow):
                # core package provisioning for V2.1 API
                if self.additional_libraries:
                    for task in element.tasks:
                        task.libraries += self.additional_libraries

            if isinstance(element, V2dot0Workflow):
                # legacy named conversion
                # existing_cluster_name -> existing_cluster_id
                if element.existing_cluster_name is not None:
                    self._adjust_legacy_existing_cluster(element)

                # core package provisioning for V2.0 API
                if self.additional_libraries:
                    element.libraries += self.additional_libraries

    def _main_traverse(self, workflows: WorkflowList):
        """
        This traverse applies all the transformations to the workflows
        :param workflows:
        :return: None
        """
        for element, parent, index in self._traverse(workflows):

            if isinstance(element, NewCluster):
                # driver_instance_pool_name -> driver_instance_pool_id
                if element.driver_instance_pool_name is not None:
                    self._adjust_legacy_driver_instance_pool_ref(element)
                # instance_pool_name -> instance_pool_id
                if element.instance_pool_name is not None:
                    self._adjust_legacy_instance_pool_ref(element)
                # instance_profile_name -> instance_profile_arn
                if element.aws_attributes is not None and element.aws_attributes.instance_profile_name is not None:
                    self._adjust_legacy_instance_profile_ref(element)

            if isinstance(element, str):
                if element.startswith("file://") or element.startswith("file:fuse://"):
                    self._adjust_file_ref(element, parent, index)

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

    def _cluster_policy_traverse(self, workflows: WorkflowList):
        """
        This traverse applies only the policy_name OR policy_id traverse.
        Please note that this traverse should go STRICTLY after all other rules,
        when ids and other transformations are already resolved.
        :param workflows:
        :return: None
        """
        for element, parent, index in self._traverse(workflows):
            if (isinstance(parent, V2dot0Workflow) or isinstance(parent, JobTaskSettings)) and isinstance(
                element, NewCluster
            ):
                if element.policy_name is not None or (
                    isinstance(element, NewCluster)
                    and element.policy_id is not None
                    and element.policy_id.startswith("cluster-policy://")
                ):
                    element = self._adjust_policy_ref(element)
                parent.new_cluster = element

    def traverse(self, workflows: Union[WorkflowList, List[str]]):
        self._library_traverse(workflows)
        self._main_traverse(workflows)
        self._cluster_policy_traverse(workflows)
