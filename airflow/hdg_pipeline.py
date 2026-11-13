"""Airflow chỉ điều phối kiểm tra đọc; không thay thuật toán hoặc cấp quyền ghi pipeline."""
from datetime import datetime,timedelta,timezone

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException, AirflowFailException


def operation_guard(operation='verify'):
    # BƯỚC 1: Không coi pool Airflow là khóa của writer ngoài Airflow.
    # Chưa có Spark Linux/maintenance fence được nghiệm thu: mọi đường ghi đều fail trước DB.
    if operation!='verify':
        raise AirflowFailException('WRITE_OPERATION_BLOCKED: use existing guarded operator procedure; no Airflow writer handoff approved')


def runtime_preflight():
    # BƯỚC 2: Instance riêng + role chỉ SELECT. Không cần secrets của owner/API/BI.
    import os,sys,json,urllib.request
    if os.environ.get('HDG_AIRFLOW_INSTANCE')!='hdg-c360':raise AirflowFailException('WRONG_AIRFLOW_INSTANCE')
    sys.path.insert(0,'/opt/hdg/jobs')
    from c360_full_load import connect
    with connect() as c:
        with c.transaction():
            c.execute('SET TRANSACTION READ ONLY')
            role,superuser,write=c.execute("""SELECT current_user,
             (SELECT rolsuper FROM pg_roles WHERE rolname=current_user),
             has_table_privilege(current_user,'c360.c360_thong_tin_ca_nhan','INSERT,UPDATE,DELETE')
             OR has_table_privilege(current_user,'public.mirror_hdg_nhankhau','INSERT,UPDATE,DELETE')""").fetchone()
            if role!='hdg_airflow_reader' or superuser or write:raise AirflowFailException('AIRFLOW_READ_ONLY_ROLE_REQUIRED')
    with urllib.request.urlopen('http://host.docker.internal:8083/connectors/hdg-oracle-observe/status',timeout=15) as response:
        status=json.load(response)
    if status['connector']['state']!='RUNNING' or not status.get('tasks') or any(t['state']!='RUNNING' for t in status['tasks']):
        raise AirflowException('DEBEZIUM_NOT_RUNNING')
    print('Runtime preflight PASS; Connector RUNNING is not a snapshot/offset barrier')


def reconcile_c360():
    # BƯỚC 3: Gọi đúng CLI đang dùng, không chép lại lọc/dedup/DQ hoặc publish trong DAG.
    import subprocess,sys,json
    run=subprocess.run([sys.executable,'-B','/opt/hdg/jobs/c360_reconcile.py'],
                       capture_output=True,text=True,timeout=540)
    if run.stderr:print('reconcile stderr present; see sanitized error_type in result')
    try:metrics=json.loads(run.stdout)
    except ValueError:raise AirflowException('INVALID_RECONCILE_OUTPUT')
    print(json.dumps(metrics,ensure_ascii=False))
    if run.returncode:
        if metrics.get('status')=='FAIL':raise AirflowFailException('C360_RECONCILE_FAILED')
        raise AirflowException('C360_RECONCILE_BLOCKED: retry after transient dependency/queue condition')
    if metrics.get('status') not in ('PASS','PASS_WITH_UNKNOWN'):raise AirflowFailException('UNVERIFIED_RECONCILE_RESULT')


def read_only_complete():
    # BƯỚC 4: Không dùng nhãn này như bằng chứng publish/cutover/E2E đã hoàn thành.
    print('READ_ONLY_CHECKS_COMPLETE: no Full Load, publish, cutover, replay, C360 write or queue ACK executed')


with DAG(
    dag_id='hdg_pipeline_checks',description='HDG: kiểm tra dependency và đối soát C360 chỉ đọc',
    schedule=None,start_date=datetime(2026,1,1,tzinfo=timezone.utc),catchup=False,
    is_paused_upon_creation=True,max_active_runs=1,max_active_tasks=1,
    dagrun_timeout=timedelta(minutes=35),tags=['hdg','read-only','manual'],
    params={'operation':Param('verify',type='string',enum=['verify','full_load','publish','cutover','cdc_replay','c360_full','c360_incremental'])},
    default_args={'owner':'hdg','retries':2,'retry_delay':timedelta(minutes=1),
                  'execution_timeout':timedelta(minutes=10),'pool':'hdg_read_only','do_xcom_push':False},
) as dag:
    guard=PythonOperator(task_id='operation_guard',python_callable=operation_guard,op_kwargs={'operation':'{{ params.operation }}'},retries=0)
    preflight=PythonOperator(task_id='runtime_preflight',python_callable=runtime_preflight)
    reconcile=PythonOperator(task_id='reconcile_c360',python_callable=reconcile_c360)
    complete=PythonOperator(task_id='read_only_complete',python_callable=read_only_complete)
    guard >> preflight >> reconcile >> complete
