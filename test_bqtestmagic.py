import os
import tempfile
import textwrap

import pandas as pd
import pytest
from google.api_core.exceptions import BadRequest
from pytest_mock.plugin import MockerFixture

from bqtestmagic import BigQueryTest, SQLTestMagic


class TestSQLTestMagic:
    @pytest.fixture
    def bqtest(self) -> SQLTestMagic:
        return SQLTestMagic()

    def test_fetch_query_result_as_dataframe_if_target_is_bigquery(
        self, mocker: MockerFixture, bqtest: SQLTestMagic
    ):
        df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
        mocker.patch("google.cloud.bigquery.Client")
        mock = mocker.patch("bqtestmagic.BigQueryTest.test", return_value=df)
        actual = bqtest.sql("BigQuery", "SELECT 1 col1, 3 col2 UNION ALL SELECT 2, 4")
        mock.assert_called_once()

        pd.testing.assert_frame_equal(df, actual)

    def test_raise_error_if_target_is_not_bigquery(self, bqtest: SQLTestMagic):
        with pytest.raises(NotImplementedError):
            bqtest.sql("other", "SELECT 1 col1")

    @pytest.mark.parametrize(
        ("line", "query", "csv_file", "sql_file", "project", "reliable"),
        [
            ("BigQuery", "SELECT 1 col1", None, None, None, False),
            (
                "BigQuery --csv_file=a.csv --sql_file=b.sql --project=my-project --reliable",  # noqa: E501
                "SELECT 1 col1",
                "a.csv",
                "b.sql",
                "my-project",
                True,
            ),
        ],
    )
    def test_parse_argstring(
        self,
        mocker: MockerFixture,
        bqtest: SQLTestMagic,
        line: str,
        query: str,
        csv_file: str,
        sql_file: str,
        project: str,
        reliable: bool,
    ):
        client = mocker.patch("google.cloud.bigquery.Client")
        mock = mocker.patch("bqtestmagic.BigQueryTest.test")
        bqtest.sql(line, query)

        client.assert_called_once_with(project)
        mock.assert_called_once_with(
            query=query, csv_file=csv_file, sql_file=sql_file, reliable=reliable
        )

    class TestClose:
        def test_close_bigquery_client_if_it_has_close_attribute(
            self, mocker: MockerFixture, bqtest: SQLTestMagic
        ):
            client = mocker.Mock(spec=["close"])
            mocker.patch("google.cloud.bigquery.Client", return_value=client)
            bqtest.sql("BigQuery", "SELECT 1 col1")

            client.close.assert_called_once_with()

        def test_no_close_if_bigquery_client_does_not_have_close_attribute(
            self, mocker: MockerFixture, bqtest: SQLTestMagic
        ):
            client = mocker.Mock(spec=[])
            mocker.patch("google.cloud.bigquery.Client", return_value=client)
            bqtest.sql("BigQuery", "SELECT 1 col1")

            assert hasattr(client, "close") is False


class TestBigQueryTest:
    @pytest.fixture
    def bigquery_test(self) -> BigQueryTest:
        return BigQueryTest(None)

    class TestTest:
        def test_raise_error_if_both_csv_file_and_sql_file_are_set(
            self, bigquery_test: BigQueryTest
        ):
            with pytest.raises(ValueError):
                bigquery_test.test(
                    query="SELECT 1",
                    csv_file="set.csv",
                    sql_file="set.sql",
                    reliable=False,
                )

        class TestDataframeQueryResultsToDataframe:
            def test_no_tests_if_both_csv_file_and_sql_file_are_not_set(
                self,
                mocker: MockerFixture,
                capfd: pytest.CaptureFixture,
                bigquery_test: BigQueryTest,
            ):
                df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
                mocker.patch("google.cloud.bigquery.Client", return_value=None)
                download_query_results_to_dataframe = mocker.patch(
                    "bqtestmagic.BigQueryTest.download_query_results_to_dataframe",
                    return_value=df,
                )
                query = "SELECT 1 col1, 3 col2 UNION ALL SELECT 2, 4"
                actual = bigquery_test.test(
                    query=query,
                    csv_file=None,
                    sql_file=None,
                    reliable=False,
                )
                download_query_results_to_dataframe.assert_called_once_with(query)
                pd.testing.assert_frame_equal(actual, df)
                assert capfd.readouterr() == ("", "")

            class TestPrintsWhetherQueryResultsAreEqualToCSVFileIfCSVFileIsSetAndSQLFileIsNotSet:  # noqa: E501
                def test_success(
                    self,
                    mocker: MockerFixture,
                    capfd: pytest.CaptureFixture,
                    bigquery_test: BigQueryTest,
                ):
                    df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
                    mocker.patch("google.cloud.bigquery.Client", return_value=None)
                    download_query_results_to_dataframe = mocker.patch(
                        "bqtestmagic.BigQueryTest.download_query_results_to_dataframe",
                        return_value=df,
                    )
                    query = "SELECT 1 col1, 3 col2 UNION ALL SELECT 2, 4"
                    with tempfile.NamedTemporaryFile("w") as f:
                        f.write("col1,col2\n1,3\n2,4")
                        f.seek(0)
                        actual = bigquery_test.test(
                            query=query,
                            csv_file=f.name,
                            sql_file=None,
                            reliable=False,
                        )
                    download_query_results_to_dataframe.assert_called_once_with(query)
                    pd.testing.assert_frame_equal(actual, df)
                    assert capfd.readouterr() == ("✓\n", "")

                def test_failure(
                    self,
                    mocker: MockerFixture,
                    capfd: pytest.CaptureFixture,
                    bigquery_test: BigQueryTest,
                ):
                    df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
                    mocker.patch("google.cloud.bigquery.Client", return_value=None)
                    download_query_results_to_dataframe = mocker.patch(
                        "bqtestmagic.BigQueryTest.download_query_results_to_dataframe",
                        return_value=df,
                    )
                    query = "SELECT 1 col1, 3 col2 UNION ALL SELECT 2, 4"
                    with tempfile.NamedTemporaryFile("w") as f:
                        f.write("col1,col2\n0,0\n0,0")
                        f.seek(0)
                        actual = bigquery_test.test(
                            query=query,
                            csv_file=f.name,
                            sql_file=None,
                            reliable=False,
                        )
                    download_query_results_to_dataframe.assert_called_once_with(query)
                    pd.testing.assert_frame_equal(actual, df)
                    assert capfd.readouterr() == ("✕\n", "")

            def test_query_validation_if_sql_file_is_set_and_not_reliable(
                self,
                mocker: MockerFixture,
                bigquery_test: BigQueryTest,
            ):
                df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
                mocker.patch("google.cloud.bigquery.Client", return_value=None)
                mocker.patch(
                    "bqtestmagic.BigQueryTest.download_query_results_to_dataframe",
                    return_value=df,
                )
                mocker.patch(
                    "bqtestmagic.BigQueryTest.query_to_check_that_two_query_results_match",  # noqa: E501
                    return_value=False,
                )
                validate_query = mocker.patch("bqtestmagic.BigQueryTest.validate_query")
                unreliable_query = "SELECT col1, col1 + 3 col2 FROM UNNEST([1, 2]) col1"
                with tempfile.NamedTemporaryFile("w") as f:
                    f.write(unreliable_query)
                    f.seek(0)
                    bigquery_test.test(
                        query="SELECT 1 col1, 3 col2 UNION ALL SELECT 2, 4",
                        csv_file=None,
                        sql_file=f.name,
                        reliable=False,
                    )

                validate_query.assert_called_once_with(unreliable_query)

            class TestPrintThatTwoQueryResultsAreEqualIfCsvFileIsNotSetAndSqlFileIsSet:
                def test_success(
                    self,
                    mocker: MockerFixture,
                    capfd: pytest.CaptureFixture,
                    bigquery_test: BigQueryTest,
                ):
                    df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
                    mocker.patch("google.cloud.bigquery.Client", return_value=None)
                    download_query_results_to_dataframe = mocker.patch(
                        "bqtestmagic.BigQueryTest.download_query_results_to_dataframe",
                        return_value=df,
                    )
                    query = "SELECT 1 col1, 3 col2 UNION ALL SELECT 2, 4"
                    with tempfile.NamedTemporaryFile("w") as f:
                        f.write("SELECT col1, col1 + 2 col2 FROM UNNEST([1, 2]) col1")
                        f.seek(0)
                        mocker.patch(
                            "bqtestmagic.BigQueryTest.query_to_check_that_two_query_results_match",  # noqa: E501
                            return_value=True,
                        )
                        actual = bigquery_test.test(
                            query=query,
                            csv_file=None,
                            sql_file=f.name,
                            reliable=True,
                        )
                    download_query_results_to_dataframe.assert_called_once_with(query)
                    pd.testing.assert_frame_equal(actual, df)
                    assert capfd.readouterr() == ("✓\n", "")

                def test_failure(
                    self,
                    mocker: MockerFixture,
                    capfd: pytest.CaptureFixture,
                    bigquery_test: BigQueryTest,
                ):
                    df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
                    mocker.patch("google.cloud.bigquery.Client", return_value=None)
                    download_query_results_to_dataframe = mocker.patch(
                        "bqtestmagic.BigQueryTest.download_query_results_to_dataframe",
                        return_value=df,
                    )
                    query = "SELECT 1 col1, 3 col2 UNION ALL SELECT 2, 4"
                    with tempfile.NamedTemporaryFile("w") as f:
                        f.write("SELECT col1, col1 + 3 col2 FROM UNNEST([1, 2]) col1")
                        f.seek(0)
                        mocker.patch(
                            "bqtestmagic.BigQueryTest.query_to_check_that_two_query_results_match",  # noqa: E501
                            return_value=False,
                        )
                        actual = bigquery_test.test(
                            query=query,
                            csv_file=None,
                            sql_file=f.name,
                            reliable=True,
                        )
                    download_query_results_to_dataframe.assert_called_once_with(query)
                    pd.testing.assert_frame_equal(actual, df)
                    assert capfd.readouterr() == ("✕\n", "")

    @pytest.mark.skipif(
        os.environ.get("CI", "false") != "true",
        reason="Unauthenticated tests only",
    )
    def test_download_query_results_to_dataframe(self, bigquery_test: BigQueryTest):
        actual = bigquery_test.download_query_results_to_dataframe(
            "SELECT 1 col1, 3 col2 UNION ALL SELECT 2, 4"
        )
        expected = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
        pd.testing.assert_frame_equal(actual, expected)

    @pytest.mark.skipif(
        os.environ.get("CI", "false") != "true",
        reason="Unauthenticated tests only",
    )
    @pytest.mark.parametrize(
        ("left", "right", "expected"),
        [
            ("SELECT 1 a", "SELECT 0 + 1 a", True),
            ("SELECT 1 a", "SELECT 1 + 1 a", False),
        ],
    )
    def test_query_to_check_that_two_query_results_match(
        self, bigquery_test: BigQueryTest, left: str, right: str, expected: bool
    ):
        actual = bigquery_test.query_to_check_that_two_query_results_match(left, right)
        assert actual == expected

    @pytest.mark.skipif(
        os.environ.get("CI", "false") != "true",
        reason="Unauthenticated tests only",
    )
    class TestValidateQuery:
        def test_pass_if_select_statement(self, bigquery_test: BigQueryTest):
            bigquery_test.validate_query("WITH t AS (SELECT 1) SELECT * FROM t")

        def test_raise_error_if_scripting(self, bigquery_test: BigQueryTest):
            with pytest.raises(ValueError):
                bigquery_test.validate_query("SELECT 1; SELECT 2;")

        def test_raise_error_if_sql_injection(self, bigquery_test: BigQueryTest):
            query = textwrap.dedent(
                """\
                SELECT　1 ) AS actual
              GROUP BY
                json_string) AS actual
            FULL JOIN (
              SELECT
                TO_JSON_STRING(expected) AS json_string,
                COUNT(*) AS count
              FROM (
                SELECT　1 ) AS expected
              GROUP BY
                json_string) AS expected
            USING
              (json_string)
            WHERE
              (actual.count = expected.count) IS NOT TRUE );
            SELECT 1;
            SELECT
              NOT EXISTS(
              SELECT
                *
              FROM (
                SELECT
                  TO_JSON_STRING(actual) AS json_string,
                  COUNT(*) AS count
                FROM (
                　　SELECT 1 ) AS actual
            """
            )

            with pytest.raises(BadRequest):
                bigquery_test.validate_query(query)
