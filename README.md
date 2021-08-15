# bqtestmagic
IPython magic cell command to test BigQuery standard SQL.
We recommend using bqtestmagic on Google Colaboratory.

## Setup

### Install Extension

```Shell
pip install git+https://github.com/na0fu3y/bqtest-magic
```

### Load Extension

```Jupyter Notebook
%load_ext bqtestmagic
```

## Usage

### Test Standard SQL using sql

```Jupyter Notebook
%%writefile expected.sql
SELECT 1 AS a, "2" AS b
UNION ALL
SELECT 2 AS a, "4" AS b
UNION ALL
SELECT 3 AS a, "6" AS b

%%sql bigquery --sql_file=expected.sql --project=your-project
SELECT a, CAST(a * 2 AS STRING) b
FROM UNNEST(GENERATE_ARRAY(1, 3)) AS a
```

> ✓
>
> |     | a | b |
> | --- |---|---|
> |**0**| 1 | 2 |
> |**1**| 2 | 4 |
> |**2**| 3 | 6 |

### Test Standard SQL using csv

```Jupyter Notebook
%%writefile expected.csv
a,b
1,2
3,4

%%sql bigquery --csv_file=expected.csv
SELECT 4 AS a, 3 AS b
UNION ALL
SELECT 2 AS a, 1 AS b
```

> ✕
>
> |     | a | b |
> | --- |---|---|
> |**0**| 4 | 3 |
> |**1**| 2 | 1 |

### Query without test

```Jupyter Notebook
%%sql bigquery --project=your-project
SELECT a, CAST(a * 3 AS STRING) b
FROM UNNEST(GENERATE_ARRAY(1, 3)) AS a
```

> |     | a | b |
> | --- |---|---|
> |**0**| 1 | 3 |
> |**1**| 2 | 6 |
> |**2**| 3 | 9 |
