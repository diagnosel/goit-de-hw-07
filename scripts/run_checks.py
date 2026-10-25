"""Run the actual DAG against the configured local MySQL and record task states."""

import json
import sys
from pathlib import Path

import pendulum
from airflow.hooks.base import BaseHook
from airflow.utils.session import create_session


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))
from aurika_hw07 import CONNECTION_ID, dag


def main():
    connection = BaseHook.get_connection(CONNECTION_ID)
    if connection.host not in {"127.0.0.1", "localhost"}:
        raise SystemExit("This runner is only for an isolated local MySQL")
    output = ROOT / "evidence" / "runs.json"
    if output.exists():
        raise SystemExit("Run evidence already exists; use a separate copy for another experiment")
    runs = []
    successful_medals = set()
    for number in range(1, 21):
        seconds = 35 if number == 1 else 5
        run = dag.test(execution_date=pendulum.now("UTC"), run_conf={"delay_seconds": seconds})
        with create_session() as session:
            instances = run.get_task_instances(session=session)
            states = {instance.task_id: instance.state for instance in instances}
        chosen = [medal for medal in ("bronze", "silver", "gold")
                  if states[f"count_{medal}"] == "success"]
        expected = "failed" if seconds == 35 else "success"
        if run.state != expected or len(chosen) != 1:
            raise RuntimeError(f"Unexpected real DAG result: {run.state}, {states}")
        if sum(states[f"count_{medal}"] == "skipped" for medal in ("bronze", "silver", "gold")) != 2:
            raise RuntimeError("The two unselected branches must be skipped")
        if states["delay"] != "success" or states["check_freshness"] != expected:
            raise RuntimeError("Delay/sensor states differ from the expected scenario")
        runs.append({"run_id": run.run_id, "delay_seconds": seconds,
                     "state": run.state, "selected": chosen[0], "tasks": states})
        output.write_text(json.dumps(runs, ensure_ascii=False, indent=2) + "\n")
        if run.state == "success":
            successful_medals.add(chosen[0])
        if len(successful_medals) == 3:
            print(f"Verified {len(runs)} real DAG runs: 35s failure and all three successful branches")
            return
    raise RuntimeError("Random draws did not cover all three successful branches within 20 runs")


if __name__ == "__main__":
    main()
