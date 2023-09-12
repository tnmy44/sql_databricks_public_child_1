def Slack_1():
    from airflow.providers.slack.operators.slack import SlackAPIPostOperator
    from datetime import timedelta

    return SlackAPIPostOperator(
        task_id = "Slack_1",
        text = "Starting the project: {{ params.project_name }} run on branch: {{ params.branch }} via job {{ params.job_name }}",
        channel = "abhyslackpub",
        slack_conn_id = "slack_default",
    )
