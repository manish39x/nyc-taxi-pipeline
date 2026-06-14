from airflow.sdk import dag, task
from pendulum import datetime
from utils import slack_reporter

@dag(
  dag_id="test_slack_dag",
  schedule=None,                              # manual trigger only
  start_date=datetime(2024, 1, 1),
  on_failure_callback= slack_reporter.send_slack_alert
)
def test_slack_dag():
  @task.python
  def report_to_slack():
    raise Exception("🧪 Test alert — Slack integration check")

  report_to_slack()

test_slack_dag()