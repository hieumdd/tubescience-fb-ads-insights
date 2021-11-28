import time

from google.cloud import bigquery

from models.AdsInsights.base import FBAdsInsights

BQ_CLIENT = bigquery.Client()


def load(
    model: FBAdsInsights,
    dataset: str,
    ads_account_id: str,
    rows: list[dict],
) -> int:
    output_rows = (
        BQ_CLIENT.load_table_from_json(
            rows,
            f"{dataset}.{model['name']}_{ads_account_id}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=model["schema"],
            ),
        )
        .result()
        .output_rows
    )
    update(dataset, model, ads_account_id)
    return output_rows


def update(dataset: str, model: FBAdsInsights, ads_account_id: str) -> None:
    BQ_CLIENT.query(
        f"""
    CREATE OR REPLACE TABLE {dataset}.{model['name']}_{ads_account_id} AS
    SELECT * EXCEPT(row_num) FROM
    (
        SELECT *,
        ROW_NUMBER() OVER (
        PARTITION BY {','.join(model['keys']['p_key'])}
        ORDER BY {model['keys']['incre_key']} DESC) AS row_num
        FROM {dataset}.{model['name']}_{ads_account_id}
    ) WHERE row_num = 1"""
    ).result()
