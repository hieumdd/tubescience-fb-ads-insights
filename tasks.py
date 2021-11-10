import os
import json
import uuid

from google.cloud import tasks_v2

TABLES = [
    "AdsInsights",
    "VideoInsights",
]

ACCOUNTS = [
    # "act_1747490262138666",
    # "act_3863802906447",
    # "act_2826795090925144",
    # "act_2469321280063553",
    # "act_2441095352866922",
    # "act_224800778174189",
    # "act_999827036824519",
    # "act_107231282794361",
    "act_542163415992887",
    # "act_1620113241414678",
    # "act_2450093448412095",
    "act_21475358",
    # "act_2975326515886538",
    # "act_1593101940714946",
    # "act_1846973575327780",
    # "act_974495872636652",
    # "act_2122590364734731",
    "act_228864947998439",
    # "act_1747608248723796",
    # "act_554350994969724",
    # "act_534707407373446",
    # "act_1055175521605003",
    # "act_10102767659534969",
    # "act_787472891724965",
    # "act_267544913961389",
    # "act_269585950412753",
    # "act_850397235748974",
    "act_1949222055320721",
    # "act_821441204654913",
    # "act_799064240225943",
    "act_10155603911388373",
    # "act_10155298978278373",
    # "act_171311423582237",
    # "act_151390555495677",
    # "act_931643893553428",
    # "act_2073906195957960",
    # "act_486123722185128",
    # "act_293229431826316",
    # "act_1132145507164475",
    # "act_565310461025287",
    # "act_688688725020204",
    # "act_431428104314510",
    # "act_365964814599154",
    # "act_204672280381775",
    # "act_1353066474788487",
    # "act_770154516413022",
    # "act_1480903842004749",
    # "act_1381496801945454",
    # "act_975894219172383",
    # "act_778541725626218",
    # "act_971805392966516",
    # "act_1851620621567208",
    # "act_1881107678600616",
    # "act_1244202448957812",
    # "act_1940963632615020",
    # "act_1822080087836709",
    # "act_1824540907590627",
    # "act_1824539784257406",
    # "act_1415072321967680",
    # "act_419877745531328",
    # "act_477369876364866",
    # "act_444541076374865",
    # "act_291409338412128",
    # "act_2490264904329072",
    # "act_1271367686360017",
    # "act_749657595491383",
    # "act_523463925057723",
    # "act_2297281490537035",
    # "act_1262883567205066",
    # "act_490299401783142",
    # "act_2671610552893113",
    # "act_2432892620372875",
    # "act_10105168",
    # "act_485342304981701",
    # "act_363248104470475",
    # "act_776066752575920",
    # "act_309287155913038",
    # "act_267421560837596",
    # "act_2050021318447381",
    # "act_2101585763473732",
    # "act_152900641498413",
    # "act_2651653758184576",
    # "act_1550063471716075",
    # "act_2102008593353224",
    # "act_1503463316533389",
    # "act_336262913664636",
    # "act_10158810768555508",
    # "act_1381240942154664",
    # "act_3635858869782761",
    # "act_1273660049448268",
    # "act_408829937124026",
    # "act_1195641163801544",
    # "act_615390636074257",
    # "act_920016725156034",
    # "act_2493883207560976",
    # "act_246311679373354",
    # "act_1223529531023664",
    # "act_1252697341443995",
    # "act_328072244639587",
    # "act_356631748425423",
    # "act_1345356512266795",
    # "act_454769095197055",
    # "act_580251625868702",
    # "act_2465796217019913",
    # "act_467322124170543",
    # "act_1576635752489340",
    # "act_611277952538796",
    # "act_1028145947312439",
    # "act_107342806058637",
    # "act_512746932198556",
    # "act_837931263323907",
    # "act_769603890495531",
]

TASKS_CLIENT = tasks_v2.CloudTasksClient()
CLOUD_TASKS_PATH = (
    os.getenv("PROJECT_ID"),
    os.getenv("REGION"),
    "fb-ads-insights",
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
        "tasks": len(responses),
        "tasks_data": tasks_data,
    }
