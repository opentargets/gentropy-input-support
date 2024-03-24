"""Shared facilities for running non-Spark ingestion on Google Batch."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from subprocess import run

import pandas as pd


@dataclass
class DataSourceBase:
    """A base dataclass to describe data source parameters for ingestion."""

    # GCP parameters.
    gcp_project = "open-targets-genetics-dev"
    gcp_region = "europe-west1"

    # GCP paths.
    gcp_staging_path = "gs://gentropy-tmp/batch/staging"
    gcp_output_path = "gs://gentropy-tmp/batch/output"

    # Data source parameters.
    max_parallelism: int = 50  # How many ingestion tasks to run concurrently.
    cpu_per_task: int = 4  # How many CPUs use per ingestion task.
    mem_per_task_gb: float = (
        4.0  # How many GB of RAM to allocate per CPU for each ingestion job.
    )

    def _get_number_of_tasks(self) -> int:
        """Return the total number of ingestion tasks for the data source.

        Returns:
            int: Total number of ingestion tasks.

        Raises:
            NotImplementedError: Always, because this method needs to be implemented by each specific data source class.
        """
        raise NotImplementedError(
            "The get_number_of_tasks() method must be implemented by data source classes."
        )

    def _generate_job_config(
        self,
        job_id: str,
        output_filename: str,
    ) -> None:
        """Generate configuration for a Google Batch job.

        Args:
            job_id (str): A unique job ID to identify a Google Batch job.
            output_filename (str): Output filename to store generated job config.
        """
        number_of_tasks = self._get_number_of_tasks()
        config = {
            "taskGroups": [
                {
                    "taskSpec": {
                        "runnables": [
                            {
                                "script": {
                                    "text": f"bash /mnt/share/code/runner.sh {job_id} {self.data_source_name}",
                                }
                            }
                        ],
                        "computeResource": {
                            "cpuMilli": self.cpu_per_task * 1000,
                            "memoryMib": int(
                                self.cpu_per_task * self.mem_per_task_gb * 1024
                            ),
                        },
                        "volumes": [
                            {
                                "gcs": {"remotePath": "gentropy-tmp/batch/staging"},
                                "mountPath": "/mnt/share",
                            }
                        ],
                        "maxRetryCount": 1,
                        "maxRunDuration": "3600s",
                    },
                    "taskCount": number_of_tasks,
                    "parallelism": min(number_of_tasks, self.max_parallelism),
                }
            ],
            "allocationPolicy": {
                "instances": [
                    {
                        "policy": {
                            "machineType": f"n2d-highmem-{self.cpu_per_task}",
                            "provisioningModel": "SPOT",
                        }
                    }
                ]
            },
            "logsPolicy": {"destination": "CLOUD_LOGGING"},
        }
        with open(output_filename, "w") as outfile:
            outfile.write(json.dumps(config, indent=4))

    def _get_study_index_location(self):
        return f"{self.gcp_output_path}/{self.data_source_name}/study_index.tsv"

    def _get_summary_stats_location(self):
        return f"{self.gcp_output_path}/{self.data_source_name}/summary_stats.parquet"

    def _get_study_index(self):
        return pd.read_table(self._get_study_index_location())

    def _get_number_of_tasks(self):
        return len(self._get_study_index())

    def deploy_code_to_storage(self) -> None:
        """Deploy code to Google Storage."""
        run(
            [
                "gsutil",
                "-m",
                "-q",
                "cp",
                "-r",
                ".",
                f"{self.gcp_staging_path}/code",
            ],
            check=False,
        )

    def submit_summary_stats_ingestion(self) -> None:
        """Submit job for processing on Google Batch."""
        # Build job ID.
        current_utc_time = datetime.now(timezone.utc)
        formatted_time = current_utc_time.strftime("%Y%m%d-%H%M%S")
        batch_safe_name = self.data_source_name.replace("_", "-")
        job_id = f"{batch_safe_name}-{formatted_time}"

        # Generate job config.
        job_config_file = tempfile.NamedTemporaryFile(delete=False)
        self._generate_job_config(job_id, job_config_file.name)

        # Submit Google Batch job.
        run(
            [
                "gcloud",
                "batch",
                "jobs",
                "submit",
                job_id,
                f"--config={job_config_file.name}",
                f"--project={self.gcp_project}",
                f"--location={self.gcp_region}",
            ],
            check=False,
        )
        os.remove(job_config_file.name)

    def ingest_study_index(self) -> None:
        """Ingests study index and stores it in a remote location."""
        raise NotImplementedError(
            "The ingest_study_index method must be implemented by data source classes."
        )

    def ingest_single_summary_stats(self, task_index: int) -> None:
        """Ingest data for a single file from the data source."""
        raise NotImplementedError(
            "The ingest_single_summary_stats() method must be implemented by data source classes."
        )
