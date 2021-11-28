import time

from google.cloud import bigquery

from models.AdsInsights.base import FBAdsInsights


def load(
    client: bigquery.Client,
    model: FBAdsInsights,
    dataset: str,
    rows: list[dict],
    attempt: int = 0,
) -> int:
    try:
        output_rows = (
            client.load_table_from_json(
                rows,
                f"{dataset}.{model['name']}",
                job_config=bigquery.LoadJobConfig(
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_APPEND",
                    schema=model["schema"],
                ),
            )
            .result()
            .output_rows
        )
        # update(client, dataset, model)
        return output_rows
    except Exception as e:
        if attempt < 10:
            time.sleep(10)
            return load(client, model, dataset, rows, attempt + 1)
        else:
            raise e


def update(client: bigquery.Client, dataset: str, model: FBAdsInsights) -> None:
    client.query(
        f"""
    CREATE OR REPLACE TABLE {dataset}.{model['name']} AS
    SELECT * EXCEPT(row_num) FROM
    (
        SELECT *,
        ROW_NUMBER() OVER (
        PARTITION BY {','.join(model['keys']['p_key'])}
        ORDER BY {model['keys']['incre_key']} DESC) AS row_num
        FROM {dataset}.{model['name']}
    ) WHERE row_num = 1"""
    ).result()
