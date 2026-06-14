from dotenv import load_dotenv
import requests
import os
load_dotenv()


SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK")
def send_slack_alert(context):
    task_instance = context.get("task_instance")
    dag_id        = context.get("dag").dag_id
    task_id       = task_instance.task_id
    run_id        = context.get("run_id")
    exception     = context.get("exception")
    log_url       = task_instance.log_url

    message = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "🚨 Airflow Task Failed"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*DAG:*\n`{dag_id}`"},
                        {"type": "mrkdwn", "text": f"*Task:*\n`{task_id}`"},
                        {"type": "mrkdwn", "text": f"*Run ID:*\n`{run_id}`"},
                        {"type": "mrkdwn", "text": f"*Error:*\n`{str(exception)[:200]}`"}
                    ]
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "View Logs"},
                            "url": log_url,
                            "style": "danger"
                        }
                    ]
                }
            ]
    }
    response = requests.post(SLACK_WEBHOOK, json=message)

    if response.status_code != 200:
        print(f"Slack alert failed: {response.status_code} {response.text}")
    else:
        print("Slack alert sent successfully")





