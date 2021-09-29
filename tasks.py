import os
import json
import uuid

from google.cloud import tasks_v2

ACCOUNTS = [
        "act_1747490262138666",
    ]

TASKS_CLIENT = tasks_v2.CloudTasksClient()
CLOUD_TASKS_PATH = (
    os.getenv("PROJECT_ID"),
    os.getenv("REGION"),
    "fb_ads_insights",
)
PARENT = TASKS_CLIENT.queue_path(*CLOUD_TASKS_PATH)

def create_tasks(tasks_data):
    """Create tasks and put into queue
    Args:
        tasks_data (dict): Task request
    Returns:
        dict: Job Response
    """

    payloads = [
        {
            "name": f"{account}-{uuid.uuid4()}",
            "payload": {
                "ads_account_id": account,
                "start": tasks_data.get("start"),
                "end": tasks_data.get("end"),
            },
        }
        for account in ACCOUNTS
    ]
    tasks = [
        {
            "name": TASKS_CLIENT.task_path(*CLOUD_TASKS_PATH, task=payload["name"]),
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": f"https://{os.getenv('REGION')}-{os.getenv('PROJECT_ID')}.cloudfunctions.net/{os.getenv('FUNCTION_NAME')}",
                "oidc_token": {
                    "service_account_email": os.getenv("GCP_SA"),
                },
                "headers": {
                    "Content-type": "application/json",
                },
                "body": json.dumps(payload["payload"]).encode(),
            },
        }
        for payload in payloads
    ]
    responses = [
        TASKS_CLIENT.create_task(
            request={
                "parent": PARENT,
                "task": task,
            }
        )
        for task in tasks
    ]
    return {
        "messages_sent": len(responses),
        "tasks_data": tasks_data,
    }
