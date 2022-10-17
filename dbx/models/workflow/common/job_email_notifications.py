from typing import Optional, List

from dbx.models.workflow.common.flexible import FlexibleModel


class JobEmailNotifications(FlexibleModel):
    on_start: Optional[List[str]]
    on_success: Optional[List[str]]
    on_failure: Optional[List[str]]
    no_alert_for_skipped_runs: Optional[bool]
