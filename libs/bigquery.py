import time
from typing import Callable

from google.cloud import bigquery

BQ_CLIENT = bigquery.Client()

Load = Callable[[str, str, list[dict]], int]


def load(
    name: str,
    schema: list[dict],
    p_key: list[str],
) -> Load:
    def _load(
        dataset: str,
        ads_account_id: str,
        rows: list[dict],
    ) -> int:
        output_rows = (
            BQ_CLIENT.load_table_from_json(
                rows,
                f"{dataset}.{name}_{ads_account_id}",
                job_config=bigquery.LoadJobConfig(
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_APPEND",
                    schema=schema,
                ),
            )
            .result()
            .output_rows
        )
        update(name, p_key, dataset, ads_account_id)
        return output_rows

    return _load


def update(name: str, p_key: list[str], dataset: str, ads_account_id: str) -> None:
    BQ_CLIENT.query(
        f"""
    CREATE OR REPLACE TABLE {dataset}.{name}_{ads_account_id} AS
    SELECT * EXCEPT(row_num) FROM
    (
        SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY {','.join(p_key)}
            ORDER BY _batched_at DESC) AS row_num
        FROM {dataset}.{name}_{ads_account_id}
    ) WHERE row_num = 1"""
    ).result()
