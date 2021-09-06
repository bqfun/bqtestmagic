import argparse
import sys
import textwrap
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
from google.cloud import bigquery
from IPython.core import magic_arguments
from IPython.core.magic import Magics, cell_magic, magics_class


class BigQueryTest:
    def __init__(self, project: Optional[str]):
        self.client = bigquery.Client(project)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        # Older versions do not have `close`
        if hasattr(self.client, "close"):
            self.client.close()

    def download_query_results_to_dataframe(self, sql: str, labels: Dict[str, str]):
        return self.client.query(
            sql, job_config=bigquery.QueryJobConfig(labels=labels)
        ).to_dataframe()

    def query_to_check_that_two_query_results_match(
        self, left: str, right: str
    ) -> bool:
        sql = textwrap.dedent(
            f"""\
            SELECT
              NOT EXISTS(
              SELECT
                *
              FROM (
                SELECT
                  FORMAT("%T", actual) AS json_string,
                  COUNT(*) AS count
                FROM (\n{textwrap.indent(left.rstrip(), "                  ")} ) AS actual
                GROUP BY
                  json_string) AS actual
              FULL JOIN (
                SELECT
                  FORMAT("%T", expected) AS json_string,
                  COUNT(*) AS count
                FROM (\n{textwrap.indent(right.rstrip(), "                  ")} ) AS expected
                GROUP BY
                  json_string) AS expected
              USING
                (json_string)
              WHERE
                (actual.count = expected.count) IS NOT TRUE )
            """  # noqa: E501
        )
        query_job = self.client.query(sql)
        return next(iter(query_job))[0]

    def validate_query(self, query: str):
        job_config = bigquery.QueryJobConfig(dry_run=True)
        query_job = self.client.query(query, job_config=job_config)

        if query_job.statement_type != "SELECT":
            raise ValueError("Statement type must be SELECT")

    def test(
        self,
        query: str,
        csv_file: Optional[Path],
        sql_file: Optional[Path],
        reliable: bool,
        labels: Dict[str, str],
    ) -> Optional[pd.DataFrame]:
        if csv_file and sql_file:
            raise ValueError("Please specify only sql_file or csv_file.")

        try:
            actual = self.download_query_results_to_dataframe(query, labels)

            if sql_file:
                with open(sql_file) as f:
                    expected_sql = f.read()
                # Do not use untrusted SQL, which can cause SQL injection!
                if not reliable:
                    self.validate_query(expected_sql)
                equals = self.query_to_check_that_two_query_results_match(
                    expected_sql,
                    query,
                )
                print("✓" if equals else "✕")
            if csv_file:
                expected_dataframe = pd.read_csv(csv_file)
                equals = expected_dataframe.equals(actual)
                print("✓" if equals else "✕")
            return actual
        except Exception as ex:
            print(f"ERROR:\n{ex}", file=sys.stderr)
            return None


def label(string):
    if "=" not in string:
        raise argparse.ArgumentTypeError(f"{string} is not KEY=VALUE")
    return tuple(string.split("=", 1))


@magics_class
class SQLTestMagic(Magics):
    @cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument("target", type=str.lower, choices=["bigquery"])
    @magic_arguments.argument("--csv_file", type=Path)
    @magic_arguments.argument("--sql_file", type=Path)
    @magic_arguments.argument("--project", type=str)
    @magic_arguments.argument("--reliable", action="store_true")
    @magic_arguments.argument("--labels", type=label, metavar="KEY=VALUE", nargs="*")
    def sql(self, line: str, query: str) -> Optional[pd.DataFrame]:
        args: argparse.Namespace = magic_arguments.parse_argstring(self.sql, line)

        with BigQueryTest(args.project) as bqtest:
            return bqtest.test(
                query=query,
                csv_file=args.csv_file,
                sql_file=args.sql_file,
                reliable=args.reliable,
                labels={k: v for k, v in args.labels} if args.labels else {},
            )


def load_ipython_extension(ipython):
    ipython.register_magics(SQLTestMagic)
