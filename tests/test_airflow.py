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

class ETLSafety(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec=importlib.util.spec_from_file_location('etl_runner','/opt/airflow/etl_runner.py')
        cls.runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(cls.runner)
        cls.bag=DagBag(dag_folder='/opt/airflow/dags',include_examples=False)
        cls.dag=cls.bag.dags['hdg_full_load_pipeline']
    def test_etl_dependency_real_steps(self):
        self.assertFalse(self.bag.import_errors)
        self.assertEqual(set(self.dag.task_ids),set(self.runner.STEPS))
        for a,b in zip(self.runner.STEPS,self.runner.STEPS[1:]):self.assertEqual(self.dag.get_task(a).downstream_task_ids,{b})
    def test_etl_schedule_guard_and_retry(self):
        self.assertEqual(self.dag.schedule_interval,'0 2 * * *')
        self.assertTrue(self.dag.is_paused_upon_creation)
        self.assertFalse(self.dag.params['allow_clone_write'])
        self.assertEqual(self.dag.max_active_runs,1)
        for t in self.dag.tasks:self.assertIsNotNone(t.execution_timeout)
        self.assertEqual(self.dag.get_task('spark_stage').retries,1)
    def request(self):return dict(step='preflight',allow_clone_write=True,target_db='hdg_airflow_1234abcd_test',source_scn=123,batch_size=1000,dag_run='manual__test')
    def test_etl_live_and_missing_authorization_block(self):
        for changes in ({'target_db':'hdg_mirror'},{'allow_clone_write':False},{'source_scn':0},{'source_scn':True},{'batch_size':0}):
            with self.subTest(changes=changes),self.assertRaises(self.runner.Blocked):self.runner.parameters(dict(self.request(),**changes))
    def test_etl_remote_missing_connection_blocks_before_process(self):
        from types import SimpleNamespace
        fn=self.dag.get_task('preflight').python_callable
        with patch('airflow.hooks.base.BaseHook.get_connection',side_effect=RuntimeError('missing')),patch('subprocess.run') as process:
            with self.assertRaises(AirflowFailException):fn('preflight',params=self.request(),dag_run=SimpleNamespace(run_id='test',run_type='manual'))
            process.assert_not_called()
    def test_etl_private_config_missing_is_blocked(self):
        with patch.object(self.runner.Path,'is_file',return_value=False),self.assertRaises(self.runner.Blocked):self.runner.configuration(self.request())
    def test_etl_spark_failure_and_timeout_not_success(self):
        import tempfile,subprocess
        with tempfile.TemporaryDirectory() as folder:
            log=Path(folder)/'spark.log';config={k:'test' for k in ('PG_JDBC_URL','PG_USER','PG_PASSWORD','ORACLE_JDBC_URL','ORACLE_USER','ORACLE_PASSWORD')}
            for mode in ('failed','timeout'):
                proc=Mock();proc.pid=123
                proc.wait.side_effect=[subprocess.TimeoutExpired('spark',1),0] if mode=='timeout' else None
                if mode=='failed':proc.wait.return_value=1
                with patch.object(self.runner,'spark_command',return_value=['spark-submit']),patch.object(self.runner.subprocess,'Popen',return_value=proc),patch.object(self.runner.os,'killpg'),self.assertRaises(self.runner.Blocked):
                    self.runner.run_spark('full_load',[],config,log,timeout=1)
    def test_etl_transport_timeout_does_not_succeed(self):
        import subprocess
        from types import SimpleNamespace
        connection=SimpleNamespace(host='host.docker.internal',login='hdg_runner',port=22,extra_dejson=dict(identity_file='/opt/airflow/etl/key',known_hosts_file='/opt/airflow/etl/known_hosts',python_path='C:/Python311/python.exe',runner_path='E:/Projects/hdg-c360-pipeline/airflow/etl_runner.py'))
        fn=self.dag.get_task('spark_stage').python_callable
        with patch('airflow.hooks.base.BaseHook.get_connection',return_value=connection),patch.object(Path,'is_file',return_value=True),patch('subprocess.run',side_effect=subprocess.TimeoutExpired('ssh',2000)):
            with self.assertRaises(AirflowException):fn('spark_stage',params=self.request(),dag_run=SimpleNamespace(run_id='test',run_type='manual'))
    def test_etl_empty_success_reply_is_rejected(self):
        from types import SimpleNamespace
        connection=SimpleNamespace(host='host.docker.internal',login='runner',port=22,extra_dejson=dict(identity_file='/opt/airflow/etl/key',known_hosts_file='/opt/airflow/etl/known_hosts',python_path='C:/Python311/python.exe',runner_path='E:/Projects/hdg-c360-pipeline/airflow/etl_runner.py'))
        fn=self.dag.get_task('spark_stage').python_callable
        with patch('airflow.hooks.base.BaseHook.get_connection',return_value=connection),patch.object(Path,'is_file',return_value=True),patch('subprocess.run',return_value=SimpleNamespace(returncode=0,stdout='{}')):
            with self.assertRaises(AirflowException):fn('spark_stage',params=self.request(),dag_run=SimpleNamespace(run_id='test',run_type='manual'))
    def test_etl_retry_never_blindly_reloads_partial_stage(self):
        import tempfile,hashlib,json
        from contextlib import nullcontext
        request=dict(self.request(),step='spark_stage');config={'ORACLE_JDBC_URL':'test-clone'}
        signature={k:request[k] for k in ('target_db','dag_run','source_scn','batch_size')};signature['oracle_url']='test-clone'
        key=hashlib.sha256((request['target_db']+'|'+request['dag_run']).encode()).hexdigest()[:24]
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);state=root/'test-results/airflow-etl'/key;state.mkdir(parents=True)
            (state/'state.json').write_text(json.dumps(dict(signature=signature,done={'preflight':{}},spark_attempt_started=True)))
            with patch.object(self.runner,'ROOT',root),patch.object(self.runner,'target',return_value=nullcontext(Mock())),patch.object(self.runner,'source_identity'),patch.object(self.runner,'run_spark') as spark:
                with self.assertRaises(self.runner.Blocked):self.runner.execute(request,config)
                spark.assert_not_called()

if __name__=='__main__':unittest.main()
