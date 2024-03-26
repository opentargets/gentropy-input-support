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
        # Read the original metadata table.
        study_index = pd.read_table(self.study_index_source)
        # Save the study index with no modifications.
        study_index.to_csv(self._get_study_index_location(), sep="\t", index=False)

    def ingest_single_summary_stats(self, task_index: int) -> None:
        # Read the study index and select one study.
        record = self._get_study_index().loc[task_index].to_dict()
        qtl_group = record["qtl_group"]
        # Process the study.
        worker = SparkPrep(
            source_stream_type="gz",
            number_of_cores=self.cpu_per_task,
            input_uri=record["ftp_path"],
            separator="\t",
            chromosome_column_name="chromosome",
            drop_columns=[],
            output_base_path=f"{self._get_summary_stats_location()}/qtl_group={qtl_group}",
        )
        worker.process()


@dataclass
class UkbPppEur(DataSourceBase):
    """A dataclass for ingesting UKB PPP (European)."""

    data_source_name = "ukb_ppp_eur"
    study_index_source = "gs://gentropy-vault/ukb-ppp/Metadata/Protein annotation/olink_protein_map_3k_v1.tsv"
    summary_stats_prefix = "gs://gentropy-vault/ukb-ppp/UKB-PPP pGWAS summary statistics/European (discovery)"

    def _process_study_index(self, row) -> tuple[str, str]:

        # Create link to summary stats.
        # Example: CLEC4D:Q8WXI8:OID20609:v1 -> CLEC4D_Q8WXI8_OID20609_v1.
        converted_id = row["UKBPPP_ProteinID"].replace(":", "_")
        # Example: CLEC4D_Q8WXI8_OID20609_v1_Inflammation.tar.
        summary_stats_filename = f"{converted_id}_{row['Panel']}.tar"
        # Example: gs://gentropy-vault/ukb-ppp/UKB-PPP pGWAS summary statistics/European (discovery)/
        summary_stats_link = f"{self.summary_stats_prefix}/{summary_stats_filename}"

        # Manual case 2: recreate study ID. This is necessary because some studies contain a measurement of a mixture of
        # proteins and need to be clarified to avoid ID duplication in the final study index. See detailed discussions
        # in https://github.com/opentargets/issues/issues/3234. Example: UKB_PPP_EUR_AMY1B_P0DTE7_OID30707_v1.
        study_id = "_".join(
            [
                "UKB_PPP_EUR",
                row["HGNC.symbol"],
                row["UniProt2"],
                row["OlinkID"],
                "v1",
            ]
        )

        return study_id, summary_stats_link

    def ingest_study_index(self) -> None:
        # Read the original metadata table.
        study_index = pd.read_table(self.study_index_source)
        # Manual case 1: exactly one study does not correspond to any available summary stats files.
        study_index = study_index[
            study_index["UKBPPP_ProteinID"] != "GLIPR1:P48060:OID31517:v1"
        ]
        # Process study index row by row.
        (
            study_index["_gentropy_study_id"],
            study_index["_gentropy_summary_stats_link"],
        ) = zip(*study_index.apply(self._process_study_index, axis=1))
        # Save the study index.
        study_index.to_csv(self._get_study_index_location(), sep="\t", index=False)

    def ingest_single_summary_stats(self, task_index: int) -> None:
        # Read the study index and select one study.
        record = self._get_study_index().loc[task_index].to_dict()
        # Process the study.
        worker = SparkPrep(
            source_stream_type = "gz_tar",
            number_of_cores=self.cpu_per_task,
            input_uri=record["_gentropy_summary_stats_link"],
            separator=" ",
            chromosome_column_name="CHROM",
            drop_columns=[],
            output_base_path=f"{self._get_summary_stats_location()}/studyId={record['_gentropy_study_id']}",
        )
        worker.process()


all_data_source_classes = [EqtlCatalogue, UkbPppEur]
data_source_look_up = {c.data_source_name: c for c in all_data_source_classes}
