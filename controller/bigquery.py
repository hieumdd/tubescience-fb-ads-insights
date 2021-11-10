from google.cloud import bigquery

DATASET = "Facebook"


def load(
    client: bigquery.Client,
    rows: list[dict],
    table: str,
    schema: list[dict],
    keys: dict,
) -> int:
    output_rows = (
        client.load_table_from_json(
            rows,
            f"{DATASET}.{table}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=schema,
            ),
        )
        .result()
        .output_rows
    )
    update(client, table, keys)
    return output_rows


def update(client: bigquery.Client, table: str, keys: dict) -> None:
    query = f"""
    CREATE OR REPLACE TABLE {DATASET}.{table} AS
    SELECT * EXCEPT(row_num) FROM
    (
        SELECT *,
        ROW_NUMBER() OVER (
        PARTITION BY {','.join(keys['p_key'])}
        ORDER BY {keys['incre_key']} DESC) AS row_num
        FROM {DATASET}.{table}
    ) WHERE row_num = 1"""
    client.query(query).result()
