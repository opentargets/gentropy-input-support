"""Definitions of all data sources which can be ingested."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from batch_common import DataSourceBase
from spark_prep import SparkPrep


@dataclass
class EqtlCatalogue(DataSourceBase):
    """A dataclass for ingesting the eQTL Catalogue."""
    data_source_name = "eqtl_catalogue"
    study_index_source = "https://raw.githubusercontent.com/eQTL-Catalogue/eQTL-Catalogue-resources/master/tabix/tabix_ftp_paths_imported.tsv"

    def ingest_study_index(self):
        """Ingest study index and store it in a remote location."""
        study_index = pd.read_table(self.study_index_source)
        study_index.to_csv(self._get_study_index_location(), sep="\t", index=False)

    def ingest_single_summary_stats(self, task_index: int) -> None:
        """Ingest a single study from the data source.

        Args:
            task_index (int): The index of the current study being ingested across all studies in the study index.
        """
        # Read the study index and select one study.
        record = self._get_study_index().loc[task_index].to_dict()
        qtl_group = record["qtl_group"]
        # Process the study.
        worker = SparkPrep(
            number_of_cores = self.cpu_per_task,
            input_uri=record["ftp_path"],
            separator="\t",
            chromosome_column_name="chromosome",
            drop_columns=[],
            output_base_path=f"{self._get_summary_stats_location()}/qtl_group={qtl_group}",
        )
        worker.process()


all_data_source_classes = [EqtlCatalogue]
data_source_look_up = {c.data_source_name: c for c in all_data_source_classes}
