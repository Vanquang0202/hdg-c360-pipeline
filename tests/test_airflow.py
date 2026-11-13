"""Chạy trong container Airflow; không khởi chạy task ghi dữ liệu."""
import unittest,sys,importlib.util
from pathlib import Path
from airflow.models import DagBag
from airflow.exceptions import AirflowFailException

class DAGSafety(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bag=DagBag(dag_folder='/opt/airflow/dags',include_examples=False)
        cls.dag=cls.bag.dags.get('hdg_pipeline_checks')
    def test_import(self):self.assertFalse(self.bag.import_errors);self.assertIsNotNone(self.dag)
    def test_manual_serialized(self):
        self.assertIsNone(self.dag.schedule_interval);self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs,1);self.assertEqual(self.dag.max_active_tasks,1)
    def test_dependency(self):
        self.assertEqual(set(self.dag.task_ids),{'operation_guard','runtime_preflight','reconcile_c360','read_only_complete'})
        for a,b in [('operation_guard','runtime_preflight'),('runtime_preflight','reconcile_c360'),('reconcile_c360','read_only_complete')]:
            self.assertEqual(self.dag.get_task(a).downstream_task_ids,{b})
    def test_retry_timeout_and_pool(self):
        for task in self.dag.tasks:
            self.assertEqual(task.pool,'hdg_read_only');self.assertIsNotNone(task.execution_timeout)
            self.assertFalse(task.do_xcom_push)
        self.assertEqual(self.dag.get_task('operation_guard').retries,0)
        self.assertEqual(self.dag.get_task('reconcile_c360').retries,2)
    def test_writes_fail_before_any_io(self):
        guard=self.dag.get_task('operation_guard').python_callable
        guard('verify')
        for mode in ['full_load','publish','cutover','cdc_replay','c360_full','c360_incremental','unknown']:
            with self.assertRaises(AirflowFailException):guard(mode)

if __name__=='__main__':unittest.main()
