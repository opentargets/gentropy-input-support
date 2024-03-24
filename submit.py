#!/usr/bin/env python3
"""A command line script to initiate single data source ingestion on Google Cloud Batch."""

from __future__ import annotations

import argparse
import logging

from cli_common import add_data_source_name_arg, resolve_data_source

parser = argparse.ArgumentParser()
add_data_source_name_arg(parser)

if __name__ == "__main__":
    logging.info("Parsing command line arguments..")
    args = parser.parse_args()

    logging.info("Look up data source class by its name...")
    data_source = resolve_data_source(args.data_source_name)

    logging.info("Ingesting study index locally and storing it in a remote location...")
    data_source.ingest_study_index()

    logging.info("Deploying code to Google Storage...")
    data_source.deploy_code_to_storage()

    logging.info("Submitting ingestion to Google Cloud Batch...")
    data_source.submit_summary_stats_ingestion()

    logging.info("Batch job has been submitted.")
