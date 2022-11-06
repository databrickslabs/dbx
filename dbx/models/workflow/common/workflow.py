from abc import ABC, abstractmethod
from typing import Optional, Union

from dbx.models.workflow.common.flexible import FlexibleModel
from dbx.models.workflow.common.job_email_notifications import JobEmailNotifications


class CronSchedule(FlexibleModel):
    quartz_cron_expression: str
    timezone_id: str
    pause_status: Optional[str]


class WorkflowBase(FlexibleModel, ABC):
    # common fields between 2.0 and 2.1
    name: str
    email_notifications: Optional[JobEmailNotifications]
    timeout_seconds: Optional[Union[int, str]]
    schedule: Optional[CronSchedule]
    max_concurrent_runs: Optional[int]
    job_id: Optional[str]

    @abstractmethod
    def get_task(self, task_key: str):
        """Abstract method to be implemented"""

    @abstractmethod
    def override_asset_based_launch_parameters(self, payload):
        """Abstract method to be implemented"""
