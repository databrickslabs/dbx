from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from dbx.api.services.jobs import JobListing, ListJobsResponse, NamedJobsService


def test_duplicated_jobs(mocker: MockerFixture):
    mocker.patch.object(
        JobListing,
        "by_name",
        MagicMock(
            return_value=ListJobsResponse(
                **{
                    "jobs": [
                        {"job_id": 1, "settings": {"name": "dup"}},
                        {"job_id": 1, "settings": {"name": "dup"}},
                    ]
                }
            )
        ),
    )
    with pytest.raises(Exception):
        NamedJobsService(api_client=MagicMock()).find_by_name("dup")
