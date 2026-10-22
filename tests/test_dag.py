"""Local checks with stubbed MySQL responses; these are not database run evidence."""

import importlib.util
import unittest
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from airflow.exceptions import AirflowException
from airflow.models import DagBag
from airflow.utils.trigger_rule import TriggerRule


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("hw07", ROOT / "dags" / "aurika_hw07.py")
hw07 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hw07)


class DagTests(unittest.TestCase):
    def test_dag_parses_without_database_connection(self):
        with patch.object(hw07.MySqlHook, "get_conn", side_effect=AssertionError("parse connected to DB")):
            bag = DagBag(str(ROOT / "dags"), include_examples=False)
        self.assertEqual(bag.import_errors, {})
        self.assertEqual(len(bag.dags["aurika_hw07_medals"].tasks), 8)

    def test_branch_uses_random_task_value(self):
        for medal in hw07.MEDALS:
            with self.subTest(medal=medal), patch.object(hw07.random, "choice", return_value=medal):
                self.assertEqual(hw07.choose_medal(), medal)
                ti = MagicMock()
                ti.xcom_pull.return_value = medal
                self.assertEqual(hw07.choose_branch(ti), "count_" + medal.lower())
        ti.xcom_pull.return_value = "other"
        with self.assertRaises(ValueError):
            hw07.choose_branch(ti)

    def test_join_and_sensor_limits(self):
        wait = hw07.dag.get_task("delay")
        sensor = hw07.dag.get_task("check_freshness")
        self.assertEqual(wait.trigger_rule, TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
        self.assertEqual(wait.upstream_task_ids, {"count_bronze", "count_silver", "count_gold"})
        self.assertEqual(sensor.trigger_rule, TriggerRule.ALL_SUCCESS)
        self.assertEqual(sensor.timeout, 10)
        self.assertEqual(sensor.execution_timeout, timedelta(seconds=20))
        self.assertEqual(sensor.retries, 0)
        self.assertFalse(sensor.soft_fail)
        self.assertEqual(hw07.dag.max_active_runs, 1)

    def test_count_uses_bound_medal_and_returns_insert_id(self):
        for medal in hw07.MEDALS:
            connection = MagicMock()
            cursor = connection.cursor.return_value.__enter__.return_value
            cursor.fetchone.return_value = (123,)
            cursor.lastrowid = 17
            with patch.object(hw07, "mysql_hook") as hook:
                hook.return_value.get_conn.return_value = connection
                self.assertEqual(hw07.count_medals(medal), 17)
            first, second = cursor.execute.call_args_list
            self.assertIn("olympic_dataset.athlete_event_results", first.args[0])
            self.assertEqual(first.args[1], (medal,))
            self.assertIn("UTC_TIMESTAMP(6)", second.args[0])
            self.assertEqual(second.args[1], (medal, 123))
            connection.commit.assert_called_once()
            connection.close.assert_called_once()

    def test_write_failure_rolls_back(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.execute.side_effect = RuntimeError("database unavailable")
        with patch.object(hw07, "mysql_hook") as hook:
            hook.return_value.get_conn.return_value = connection
            with self.assertRaises(RuntimeError):
                hw07.count_medals("Gold")
        connection.rollback.assert_called_once()
        connection.commit.assert_not_called()
        connection.close.assert_called_once()

    def test_real_sql_sensor_poke_with_stubbed_database_results(self):
        sensor = hw07.dag.get_task("check_freshness")
        self.assertIn("TIMESTAMPDIFF(MICROSECOND, created_at, UTC_TIMESTAMP(6))", sensor.sql)
        self.assertNotIn("/", sensor.sql)
        for age in (0, 5_000_000, 30_000_000):
            hook = MagicMock()
            hook.get_records.return_value = [(age,)]
            with patch.object(sensor, "_get_hook", return_value=hook):
                self.assertTrue(sensor.poke({}))
        for records in ([(30_000_001,)], [(35_000_000,)], [(-1,)], [(None,)], []):
            hook = MagicMock()
            hook.get_records.return_value = records
            with patch.object(sensor, "_get_hook", return_value=hook):
                with self.assertRaises(AirflowException):
                    sensor.poke({})

    def test_sensor_parameters_scope_to_current_run(self):
        sensor = hw07.dag.get_task("check_freshness")
        original = sensor.parameters
        try:
            for medal in hw07.MEDALS:
                ti = SimpleNamespace(xcom_pull=lambda task_ids: medal if task_ids == "choose_medal" else 17)
                sensor.parameters = original
                sensor.render_template_fields({"ti": ti})
                self.assertEqual(sensor.parameters, ["17", medal])
                self.assertIn("WHERE id = %s AND medal_type = %s", sensor.sql)
        finally:
            sensor.parameters = original

    def test_delay_calls_sleep_with_requested_seconds(self):
        with patch.object(hw07.time, "sleep") as sleep:
            hw07.delay({"delay_seconds": 35})
            sleep.assert_called_once_with(35)
        for seconds in (-1, 61, True, "35"):
            with self.assertRaises(ValueError):
                hw07.delay({"delay_seconds": seconds})


if __name__ == "__main__":
    unittest.main(verbosity=2)
