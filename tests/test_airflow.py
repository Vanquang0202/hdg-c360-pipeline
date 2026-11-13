"""Chạy trong container Airflow; không khởi chạy task ghi dữ liệu."""
import unittest,sys,importlib.util
from pathlib import Path
from airflow.models import DagBag
from airflow.exceptions import AirflowFailException,AirflowException,AirflowSensorTimeout
from unittest.mock import patch,Mock
from airflow.sensors.python import PythonSensor

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
        tasks=['operation_guard','runtime_preflight','await_mirror','await_c360','reconcile_c360','verify_feed','serving_checks','read_only_complete']
        self.assertEqual(set(self.dag.task_ids),set(tasks))
        for a,b in zip(tasks,tasks[1:]):self.assertEqual(self.dag.get_task(a).downstream_task_ids,{b})
    def test_retry_timeout_and_pool(self):
        for task in self.dag.tasks:
            self.assertEqual(task.pool,'hdg_read_only');self.assertIsNotNone(task.execution_timeout)
            self.assertEqual(task.do_xcom_push,task.task_id in ('runtime_preflight','verify_feed'))
        self.assertEqual(self.dag.get_task('operation_guard').retries,0)
        self.assertEqual(self.dag.get_task('reconcile_c360').retries,2)
    def test_writes_fail_before_any_io(self):
        guard=self.dag.get_task('operation_guard').python_callable
        guard('verify')
        for mode in ['full_load','publish','cutover','cdc_replay','c360_full','c360_incremental','unknown']:
            with self.assertRaises(AirflowFailException):guard(mode)

    def functions(self):return self.dag.get_task('await_mirror').python_callable.__globals__
    def test_stopped_services_and_quarantine_fail(self):
        require=self.functions()['require_services']
        require(1,1,0)
        for args in ((0,1,0),(1,0,0),(1,1,1),(1,1,0,False)):
            with self.subTest(args=args),self.assertRaises(AirflowException):require(*args)
    def test_truncated_uuid_cannot_be_accepted_as_native_id(self):
        from types import SimpleNamespace
        require=self.functions()['require_topic']
        actual=SimpleNamespace(topic_id='uEcVHRQQSP6vcF2gnSDEag',partitions=[SimpleNamespace(id=0)])
        with self.assertRaises(AirflowFailException):require(actual,'AAAAABQQSP7/////nSDEag',0)
        require(actual,str(actual.topic_id),0)
    def test_checkpoint_lag_and_identity(self):
        reached=self.functions()['offsets_reached'];goal=[dict(topic='t',partition=0,topic_id='id',target=3)]
        self.assertFalse(reached(goal,[('t','id',0,2)]));self.assertTrue(reached(goal,[('t','id',0,3)]))
        with self.assertRaises(AirflowFailException):reached(goal,[('t','recreated',0,3)])
    def test_reconcile_cannot_report_false_success(self):
        validate=self.functions()['validate_reconcile']
        metrics=dict(status='PASS_WITH_UNKNOWN',missing_citizens=0,extra_citizens=0,representative_mismatches=0,normalized_mismatches=0,map_mismatches=0,expected_citizens=2,c360_rows=2)
        validate(metrics,0)
        with self.assertRaises(AirflowFailException):validate(dict(metrics,normalized_mismatches=1),0)
        with self.assertRaises(AirflowException):validate(dict(metrics,status='BLOCKED'),1)
    def test_sensors_have_bounded_timeout(self):
        for name in ('await_mirror','await_c360'):
            task=self.dag.get_task(name);self.assertEqual(task.mode,'reschedule');self.assertEqual(task.timeout,180)
        sensor=PythonSensor(task_id='timeout_test_only',python_callable=lambda:False,timeout=.001,poke_interval=.001,mode='poke',retries=0)
        with self.assertRaises(AirflowSensorTimeout):sensor.execute(context={})
    def test_http_failure_is_not_success(self):
        import os,uuid
        from urllib.error import URLError
        call=self.functions()['serving_checks']
        with patch.dict(os.environ,{'HDG_C360_API_URL':'http://host.docker.internal:8090','HDG_C360_API_KEY':uuid.uuid4().hex}),patch('urllib.request.urlopen',side_effect=URLError('simulated service unavailable')):
            with self.assertRaises(URLError):call(Mock())

if __name__=='__main__':unittest.main()
