from abc import ABC
from typing import Optional, List

from dbx.models.workflow._flexible import FlexibleModel


class JobEmailNotifications(FlexibleModel):
    on_start: Optional[List[str]]
    on_success: Optional[List[str]]
    on_failure: Optional[List[str]]
    no_alert_for_skipped_runs: Optional[bool]


class CronSchedule(FlexibleModel):
    quartz_cron_expression: str
    timezone_id: str
    pause_status: str


class WorkflowBase(FlexibleModel, ABC):
    # common fields between 2.0 and 2.1
    name: str
    email_notifications: Optional[JobEmailNotifications]
    timeout_seconds: Optional[int]
    schedule: Optional[CronSchedule]
    max_concurrent_runs: Optional[int]
