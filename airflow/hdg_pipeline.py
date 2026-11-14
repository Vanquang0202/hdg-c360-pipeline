"""Airflow chỉ điều phối kiểm tra đọc; không thay thuật toán hoặc cấp quyền ghi pipeline."""
from datetime import datetime,timedelta,timezone

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
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
    print('Runtime preflight PASS; bounded Kafka goal, not an Oracle SCN barrier')
    return capture_goal()


def reconcile_c360():
    # BƯỚC 3: Gọi đúng CLI đang dùng, không chép lại lọc/dedup/DQ hoặc publish trong DAG.
    import subprocess,sys,json
    run=subprocess.run([sys.executable,'-B','/opt/hdg/jobs/c360_reconcile.py'],
                       capture_output=True,text=True,timeout=540)
    if run.stderr:print('reconcile stderr present; see sanitized error_type in result')
    try:metrics=json.loads(run.stdout)
    except ValueError:raise AirflowException('INVALID_RECONCILE_OUTPUT')
    print(json.dumps(metrics,ensure_ascii=False))
    validate_reconcile(metrics,run.returncode)


def read_only_complete():
    # BƯỚC 4: Không dùng nhãn này như bằng chứng publish/cutover/E2E đã hoàn thành.
    print('PIPELINE_CHECKPOINT_REACHED: Kafka goal, Mirror/C360/feed reconciliation and API/BI verified; services remain externally supervised')


def db():
    import sys
    sys.path.insert(0,'/opt/hdg/jobs')
    sys.path.insert(0,'/opt/hdg/cdc/sink')
    from c360_full_load import connect
    return connect()


def require_topic(description,expected,partition,topic=None,cluster=None,bindings=None):
    import sys
    sys.path.insert(0,'/opt/hdg/cdc/sink')
    from kafka_reader import assert_topic
    from protocol import Rejected
    if partition!=0 or {x.id for x in description.partitions}!={0}:raise AirflowFailException('KAFKA_PARTITIONS_CHANGED')
    try:assert_topic(topic,expected,str(description.topic_id),cluster,bindings)
    except Rejected as error:raise AirflowFailException(error.code) from error


def capture_goal():
    # Chụp offset vector hữu hạn. Không subscribe/commit Kafka, không lấy latest làm bootstrap.
    import os
    from confluent_kafka import TopicCollection,TopicPartition
    from confluent_kafka.admin import AdminClient,OffsetSpec
    with db() as c:
        rows=c.execute("SELECT c.topic,c.topic_id,c.partition,c.next_offset FROM sink_meta.checkpoints c JOIN sink_meta.epochs e USING(epoch) WHERE e.state='ACTIVE' ORDER BY c.topic").fetchall()
        from kafka_reader import load_bindings,native_metadata
        epochs=c.execute("SELECT epoch FROM sink_meta.epochs WHERE state='ACTIVE'").fetchall()
        if len(epochs)!=1:raise AirflowFailException('ONE_ACTIVE_EPOCH_REQUIRED')
        bindings=load_bindings(c,epochs[0][0])
    if len(rows)!=5:raise AirflowFailException('FIVE_ACTIVE_STREAMS_REQUIRED')
    admin=AdminClient({'bootstrap.servers':os.environ['HDG_KAFKA_BOOTSTRAP']})
    cluster,native,descriptions=native_metadata(admin,[r[0] for r in rows])
    offsets=admin.list_offsets({TopicPartition(t,p):OffsetSpec.latest() for t,tid,p,n in rows})
    goal=[]
    for t,tid,p,n in rows:
        description=descriptions[t]
        require_topic(description,tid,p,t,cluster,bindings)
        high=offsets[TopicPartition(t,p)].result(timeout=15).offset
        if high<n:raise AirflowFailException('CHECKPOINT_AHEAD_OF_KAFKA')
        goal.append(dict(topic=t,topic_id=tid,partition=p,target=high))
    return goal


def offsets_reached(goal,rows):
    actual={(t,p):(tid,n) for t,tid,p,n in rows}
    if len(actual)!=len(goal):raise AirflowFailException('CHECKPOINT_STREAMS_CHANGED')
    for g in goal:
        item=actual.get((g['topic'],g['partition']))
        if not item or item[0]!=g['topic_id']:raise AirflowFailException('CHECKPOINT_TOPIC_CHANGED')
        if item[1]<g['target']:return False
    return True


def require_services(sink,watcher,quarantine,catalog_ok=True):
    if quarantine:raise AirflowFailException('CDC_QUARANTINE_REQUIRES_OPERATOR')
    if sink!=1:raise AirflowException('CDC_SINK_NOT_RUNNING: start guarded watch; no offset reset')
    if watcher!=1:raise AirflowException('C360_WATCHER_NOT_RUNNING: restart existing watcher')
    if not catalog_ok:raise AirflowFailException('C360_CATALOG_REFRESH_REQUIRED')


def await_mirror(ti):
    # Chỉ quan sát guard có sẵn trên snapshot nhất quán; không lấy row-lock hay ghi checkpoint.
    import sys
    sys.path[:0]=['/opt/hdg/jobs','/opt/hdg/cdc/sink']
    import cutover,storage
    with db() as c,c.transaction():
        c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        record=c.execute("SELECT plan,last_data,before_ownership FROM hdg_load.cutovers WHERE state='ACTIVE'").fetchall()
        if len(record)!=1:raise AirflowFailException('ONE_ACTIVE_CUTOVER_REQUIRED')
        plan,data,owner=record[0]
        with cutover.target(c,plan,'hdg_mirror','status'):
            storage.assert_target(c,observer=True);cutover.safe_schema(c,observer=True);cutover.assert_handoff(c,plan,read_only=True)
            if not cutover.ownership_matches(c,owner) or cutover.table_hashes(c)!=data:raise AirflowFailException('MIRROR_SCHEMA_ACL_OR_CONTENT_CHANGED')
        sink=c.execute("SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE l.locktype='advisory' AND l.classid=631874 AND l.objid=2 AND l.granted AND a.application_name='hdg_cdc_watch'").fetchone()[0]
        quarantine=c.execute('SELECT count(id) FROM sink_meta.quarantine').fetchone()[0]
        require_services(sink,1,quarantine)
        reached=offsets_reached(ti.xcom_pull(task_ids='runtime_preflight'),cutover.checkpoints(c,plan['manifest']['epoch']))
        print('Mirror checkpoint reached:',reached)
        return reached


def await_c360():
    import sys
    sys.path.insert(0,'/opt/hdg/jobs')
    from c360_full_load import catalog_digest
    with db() as c,c.transaction():
        c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        watcher=c.execute("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND classid=36012026 AND objid=1 AND granted").fetchone()[0]
        running,digest=c.execute("SELECT full_running,catalog_digest FROM c360.c360_sync_control WHERE control_key='NORMALIZE'").fetchone()
        if running:raise AirflowException('C360_FULL_RUNNING: do not overlap orchestration')
        require_services(1,watcher,0,digest==catalog_digest(c))
        queue=c.execute('SELECT count(*) FROM c360.c360_change_queue').fetchone()[0]
        print('C360 pending queue:',queue)
        return queue==0


def validate_reconcile(metrics,returncode):
    required=('missing_citizens','extra_citizens','representative_mismatches','normalized_mismatches','map_mismatches')
    if metrics.get('status')=='BLOCKED':raise AirflowException('C360_RECONCILE_BLOCKED')
    if returncode or metrics.get('status') not in ('PASS','PASS_WITH_UNKNOWN') or any(metrics.get(k)!=0 for k in required) or metrics.get('expected_citizens')!=metrics.get('c360_rows'):
        raise AirflowFailException('C360_RECONCILE_FAILED')


def verify_feed():
    with db() as c,c.transaction():
        c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        feed,high,floor=c.execute('SELECT feed_id,last_id,retained_after FROM c360.change_feed_control').fetchone()
        count,low,maximum=c.execute('SELECT count(event_id),min(event_id),max(event_id) FROM c360.change_events').fetchone()
        if count!=high-floor or (count and (low!=floor+1 or maximum!=high)):raise AirflowFailException('CHANGE_FEED_GAP')
        return dict(feed=str(feed),high=high)


def serving_checks(ti):
    import os,json,urllib.request
    import sys
    sys.path.insert(0,'/opt/hdg/jobs')
    from c360_bi import verify_http
    base=os.environ['HDG_C360_API_URL']
    if base!='http://host.docker.internal:8090':raise AirflowFailException('HDG_API_URL_REQUIRED')
    request=urllib.request.Request(base+'/api/v1/ca-nhan/thay-doi?cursor=now&limit=1',headers={'X-API-Key':os.environ['HDG_C360_API_KEY']})
    with urllib.request.urlopen(request,timeout=20) as response:body=json.load(response)
    expected=ti.xcom_pull(task_ids='verify_feed')
    if body.get('consistency')!='C360_COMMITTED_CHANGE_LOG' or body.get('feedId')!=expected['feed'] or body.get('until',-1)<expected['high'] or not body.get('nextCursor'):
        raise AirflowFailException('API_FEED_NOT_CURRENT')
    verify_http('http://host.docker.internal:8089',os.environ['HDG_SUPERSET_USERNAME'],os.environ['HDG_SUPERSET_PASSWORD'])
    print('Java API authenticated feed and HDG dashboard/chart-data PASS')


with DAG(
    dag_id='hdg_pipeline_checks',description='HDG: chờ checkpoint hữu hạn, đối soát C360/feed và kiểm API/BI',
    schedule=None,start_date=datetime(2026,1,1,tzinfo=timezone.utc),catchup=False,
    is_paused_upon_creation=True,max_active_runs=1,max_active_tasks=1,
    dagrun_timeout=timedelta(minutes=35),tags=['hdg','read-only','manual'],
    params={'operation':Param('verify',type='string',enum=['verify','full_load','publish','cutover','cdc_replay','c360_full','c360_incremental'])},
    default_args={'owner':'hdg','retries':2,'retry_delay':timedelta(minutes=1),
                  'execution_timeout':timedelta(minutes=10),'pool':'hdg_read_only','do_xcom_push':False},
) as dag:
    guard=PythonOperator(task_id='operation_guard',python_callable=operation_guard,op_kwargs={'operation':'{{ params.operation }}'},retries=0)
    preflight=PythonOperator(task_id='runtime_preflight',python_callable=runtime_preflight,do_xcom_push=True)
    mirror=PythonSensor(task_id='await_mirror',python_callable=await_mirror,mode='reschedule',poke_interval=10,timeout=180)
    c360=PythonSensor(task_id='await_c360',python_callable=await_c360,mode='reschedule',poke_interval=10,timeout=180)
    reconcile=PythonOperator(task_id='reconcile_c360',python_callable=reconcile_c360)
    feed=PythonOperator(task_id='verify_feed',python_callable=verify_feed,do_xcom_push=True)
    serving=PythonOperator(task_id='serving_checks',python_callable=serving_checks)
    complete=PythonOperator(task_id='read_only_complete',python_callable=read_only_complete)
    guard >> preflight >> mirror >> c360 >> reconcile >> feed >> serving >> complete

# DAG ETL hữu hạn riêng; tám task của hdg_pipeline_checks phía trên giữ nguyên.
def dispatch_etl(step, **context):
    import json,re,subprocess
    from pathlib import Path
    from airflow.hooks.base import BaseHook
    params=context['params'];run=context['dag_run']
    if not params.get('allow_clone_write'):
        raise AirflowFailException('WRITE_NOT_AUTHORIZED: explicitly approve an isolated clone run')
    if not re.fullmatch(r'hdg_airflow_[0-9a-f]{8}_test',params.get('target_db','')):
        raise AirflowFailException('LIVE_OR_UNKNOWN_TARGET_BLOCKED')
    if type(params.get('source_scn')) is not int or params['source_scn']<1:
        raise AirflowFailException('EXPLICIT_CLONE_SCN_REQUIRED')
    try:connection=BaseHook.get_connection('hdg_etl_runner')
    except Exception as error:raise AirflowFailException('ETL_RUNNER_CONNECTION_MISSING') from error
    extra=connection.extra_dejson
    if str(run.run_type) not in ('manual','DagRunType.MANUAL') and extra.get('allow_scheduled_clone') is not True:
        raise AirflowFailException('SCHEDULED_WRITE_NOT_APPROVED')
    # SSH có sẵn trong image; không mở Docker socket hay thêm service nhận lệnh Windows.
    host=connection.host or '';user=connection.login or ''
    if not re.fullmatch(r'[A-Za-z0-9.-]+',host) or not re.fullmatch(r'[A-Za-z0-9_.-]+',user):
        raise AirflowFailException('ETL_SSH_ENDPOINT_INVALID')
    paths=[extra.get(k,'') for k in ('identity_file','known_hosts_file')]
    if not all(Path(p).is_file() and str(Path(p)).startswith('/opt/airflow/') for p in paths):
        raise AirflowFailException('ETL_SSH_PINNED_KEY_AND_KNOWN_HOSTS_REQUIRED')
    python=extra.get('python_path','');runner=extra.get('runner_path','')
    if not all(re.fullmatch(r'[A-Za-z0-9_./:\\-]+',p) for p in (python,runner)) or not runner.replace('\\','/').endswith('/airflow/etl_runner.py'):
        raise AirflowFailException('ETL_REMOTE_RUNNER_PATH_INVALID')
    request=dict(step=step,dag_run=run.run_id,**{k:params[k] for k in ('target_db','source_scn','batch_size','allow_clone_write')})
    command=['/usr/bin/ssh','-T','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=10','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3','-o','UserKnownHostsFile='+paths[1],'-i',paths[0],'-p',str(connection.port or 22),user+'@'+host,python+' '+runner]
    try:completed=subprocess.run(command,input=json.dumps(request),capture_output=True,text=True,timeout=2000)
    except subprocess.TimeoutExpired as error:raise AirflowException('ETL_TRANSPORT_TIMEOUT: inspect remote state before retry') from error
    try:result=json.loads(completed.stdout)
    except ValueError as error:raise AirflowException('ETL_TRANSPORT_FAILED_NO_RESULT: inspect SSH and runner; no success inferred') from error
    if completed.returncode or result.get('status')=='BLOCKED':
        reason=result.get('reason','ETL_RUNNER_FAILED')
        # Chỉ log mã lỗi, không echo stderr/config/credential từ máy runner.
        if not re.fullmatch(r'[A-Z0-9_]+',reason):reason='ETL_RUNNER_FAILED'
        raise AirflowException(reason)
    allowed={'preflight':{'READY'},'spark_stage':{'STAGED'},'reconcile_stage':{'VALIDATED'},
             'publish_mirror':{'PUBLISHED'},'c360_full':{'SUCCESS','SUCCESS_WITH_UNKNOWN'},
             'reconcile_final':{'PASS','PASS_WITH_UNKNOWN'}}
    actual=result.get('previous_result',{}) if result.get('cached') is True else result
    if result.get('step')!=step or actual.get('status') not in allowed[step]:
        raise AirflowException('ETL_RESULT_MISSING_OR_WRONG_PHASE')
    if result.get('cached') and result.get('processed_this_attempt')!=0:
        raise AirflowException('ETL_CACHED_RESULT_CANNOT_CLAIM_NEW_ROWS')
    print(json.dumps(result,ensure_ascii=False))
    return result


import os as _etl_os
with DAG(
    dag_id='hdg_full_load_pipeline',description='Spark Stage → đối soát → guarded bootstrap clone → C360 Full → đối soát',
    schedule=_etl_os.environ.get('HDG_FULL_LOAD_CRON','0 2 * * *'),
    start_date=datetime(2026,1,1,tzinfo=timezone(timedelta(hours=7))),catchup=False,
    is_paused_upon_creation=True,max_active_runs=1,max_active_tasks=1,
    dagrun_timeout=timedelta(hours=4),tags=['hdg','etl','clone-only'],
    params={'allow_clone_write':Param(False,type='boolean'),
            'target_db':Param('',type='string'),
            'source_scn':Param(0,type='integer',minimum=0),
            'batch_size':Param(1000,type='integer',minimum=1,maximum=10000)},
    default_args={'owner':'hdg','retries':1,'retry_delay':timedelta(minutes=2),
                  'execution_timeout':timedelta(minutes=35),'do_xcom_push':True},
) as full_load_dag:
    _etl_tasks=[PythonOperator(task_id=step,python_callable=dispatch_etl,op_kwargs={'step':step},
                              retries=0 if step=='preflight' else 1)
                for step in ('preflight','spark_stage','reconcile_stage','publish_mirror','c360_full','reconcile_final')]
    for _previous,_next in zip(_etl_tasks,_etl_tasks[1:]):_previous >> _next
