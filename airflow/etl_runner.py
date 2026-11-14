"""Runner hữu hạn cho DAG ETL clone; tái sử dụng Spark/publisher/C360, không mở đường ghi live."""
import hashlib,json,os,re,signal,subprocess,sys,time
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlparse
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'jobs'))
STEPS=('preflight','spark_stage','reconcile_stage','publish_mirror','c360_full','reconcile_final')
MARKER='HDG_AIRFLOW_ETL_CLONE_V1'
class Blocked(RuntimeError):pass

def require(ok,reason):
    if not ok:raise Blocked(reason)

def parameters(request):
    require(request.get('step') in STEPS,'UNKNOWN_ETL_STEP')
    require(request.get('allow_clone_write') is True,'WRITE_NOT_AUTHORIZED')
    require(bool(re.fullmatch(r'hdg_airflow_[0-9a-f]{8}_test',request.get('target_db',''))),'LIVE_OR_UNKNOWN_TARGET_BLOCKED')
    require(isinstance(request.get('source_scn'),int) and not isinstance(request['source_scn'],bool) and request['source_scn']>0,'EXPLICIT_CLONE_SCN_REQUIRED')
    require(type(request.get('batch_size')) is int and 1<=request['batch_size']<=10000,'BATCH_SIZE')
    require(isinstance(request.get('dag_run'),str) and 1<=len(request['dag_run'])<=250,'DAG_RUN_REQUIRED')
    return request

def configuration(request):
    # BƯỚC 1: File riêng trên runner; không fallback jobs/.env đang trỏ Mirror live.
    from dotenv import dotenv_values
    file=ROOT/'airflow/.env.etl'
    require(file.is_file(),'ETL_PRIVATE_CONFIG_MISSING')
    config=dict(dotenv_values(file,interpolate=False))
    for k in ('PG_JDBC_URL','PG_USER','PG_PASSWORD','ORACLE_JDBC_URL','ORACLE_USER','ORACLE_PASSWORD','ORACLE_TEST_PDB'):
        require(bool(config.get(k)),'ETL_CONFIG_MISSING_'+k)
    pg=urlparse(config['PG_JDBC_URL'].removeprefix('jdbc:'))
    require(pg.scheme=='postgresql' and pg.hostname in ('localhost','127.0.0.1') and pg.port in (15432,15433) and pg.path=='/'+request['target_db'] and not(pg.username or pg.password or pg.query or pg.fragment),'ETL_PG_ENDPOINT')
    pdb=config['ORACLE_TEST_PDB']
    require(bool(re.fullmatch(r'HDG_ETL_[0-9A-F]{8}',pdb)) and config['ORACLE_JDBC_URL'].startswith('jdbc:oracle:thin:@//') and config['ORACLE_JDBC_URL'].endswith('/'+pdb),'ISOLATED_ORACLE_PDB_REQUIRED')
    require(config['ORACLE_USER']=='VSS','SOURCE_SCHEMA_MUST_BE_VSS')
    return config

@contextmanager
def target(config,request):
    # BƯỚC 2: Kiểm clone thực tế trước mọi ghi; khóa ETL, Sink và Watcher theo DB, không khóa live.
    import psycopg
    pg=urlparse(config['PG_JDBC_URL'].removeprefix('jdbc:'))
    require(pg.hostname in ('127.0.0.1','localhost') and pg.port in (15432,15433) and pg.path=='/'+request['target_db'] and bool(re.fullmatch(r'/hdg_airflow_[0-9a-f]{8}_test',pg.path)),'LIVE_OR_UNKNOWN_TARGET_BLOCKED')
    with psycopg.connect(host=pg.hostname,port=pg.port,dbname=request['target_db'],user=config['PG_USER'],password=config['PG_PASSWORD'],autocommit=True,connect_timeout=10,options='-c lock_timeout=10s -c statement_timeout=180s') as c:
        marker=c.execute("SELECT current_database(),shobj_description(oid,'pg_database') FROM pg_database WHERE datname=current_database()").fetchone()
        require(marker==(request['target_db'],MARKER),'CLONE_MARKER_REQUIRED')
        for a,b in ((784027,9),(784027,2),(631874,2),(36012026,1)):
            require(c.execute('SELECT pg_try_advisory_lock(%s,%s)',(a,b)).fetchone()[0],'OTHER_WRITER_ACTIVE')
        require(c.execute('SELECT cdc_enabled FROM hdg_load.control WHERE id=1').fetchone()==(False,),'CDC_HANDOFF_REQUIRED_USE_MANUAL_CUTOVER')
        require(c.execute("SELECT to_regclass('sink_meta.epochs')").fetchone()[0] is None,'EXISTING_SINK_EPOCH_BLOCKED')
        require(not c.execute("SELECT 1 FROM pg_trigger WHERE tgrelid='public.mirror_hdg_nhankhau'::regclass AND NOT tgisinternal").fetchone(),'MIRROR_TRIGGER_REQUIRES_SEPARATE_REHEARSAL')
        require(c.execute("SELECT full_running FROM c360.c360_sync_control WHERE control_key='NORMALIZE'").fetchone()==(False,),'C360_FULL_ALREADY_RUNNING')
        c.execute('SELECT hdg_load.assert_mirror_schema()')
        yield c

def source_identity(config):
    # Chỉ SELECT identity trên PDB test khác FREEPDB1. Không sinh/sửa Oracle trong DAG.
    import oracledb
    dsn=config['ORACLE_JDBC_URL'].removeprefix('jdbc:oracle:thin:@//')
    with oracledb.connect(user=config['ORACLE_USER'],password=config['ORACLE_PASSWORD'],dsn=dsn) as c:
        with c.cursor() as cursor:
            cursor.execute("SELECT sys_context('USERENV','CON_NAME') FROM dual")
            require(cursor.fetchone()[0]==config['ORACLE_TEST_PDB'],'SOURCE_PDB_IDENTITY_MISMATCH')

def spark_command(job,args):
    require(job in ('full_load','reconcile'),'UNSUPPORTED_JOB')
    if os.name=='nt':
        # Giữ launcher Windows đã PASS, bao gồm local[2] và classpath tránh Hadoop fetchFile.
        quote=lambda s:"'"+str(s).replace("'","''")+"'"
        command='& '+quote(ROOT/'jobs/run_spark.ps1')+' -Job '+quote(job)+' -Python '+quote(sys.executable)+' -JobArguments @('+','.join(map(quote,args))+')'
        return ['powershell.exe','-NoProfile','-NonInteractive','-Command',command]
    import pyspark
    home=Path(pyspark.__file__).parent
    jars=os.pathsep.join(str(ROOT/'jobs/jars'/p) for p in ('ojdbc11.jar','postgresql-42.7.14.jar'))
    return [str(home/'bin/spark-submit'),'--master','local[2]','--driver-memory','1g','--driver-class-path',jars,'--conf','spark.executor.extraClassPath='+jars,'--conf','spark.jars=','--conf','spark.driver.host=127.0.0.1','--conf','spark.driver.bindAddress=127.0.0.1','--conf','spark.task.maxFailures=1','--conf','spark.speculation=false',str(ROOT/'jobs'/f'{job}.py'),*args]

def run_spark(job,args,config,logfile,timeout=1800):
    # BƯỚC 3: Spark/JDBC copy dữ liệu; stdout/stderr riêng trên runner, không đẩy secret/trace vào Airflow.
    env=dict(os.environ,**{k:config[k] for k in ('PG_JDBC_URL','PG_USER','PG_PASSWORD','ORACLE_JDBC_URL','ORACLE_USER','ORACLE_PASSWORD')})
    env.update(PYSPARK_PYTHON=sys.executable,PYSPARK_DRIVER_PYTHON=sys.executable)
    with logfile.open('w',encoding='utf-8') as log:
        proc=subprocess.Popen(spark_command(job,args),env=env,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,start_new_session=os.name!='nt')
        try:code=proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            if os.name=='nt':subprocess.run(['taskkill','/PID',str(proc.pid),'/T','/F'],capture_output=True)
            else:os.killpg(proc.pid,signal.SIGKILL)
            proc.wait();raise Blocked('SPARK_TIMEOUT_INSPECT_STAGE_BEFORE_RETRY')
    require(code==0,'SPARK_FAILED_INSPECT_RUNNER_LOG')
    events=[]
    for line in logfile.read_text(encoding='utf-8',errors='replace').splitlines():
        try:value=json.loads(line)
        except ValueError:continue
        if isinstance(value,dict) and 'event' in value:events.append(value)
    return events

def audits(c,run):
    rows=c.execute('SELECT source_table,source_rows,stage_rows,content_equal,stage_digest FROM hdg_load.table_runs WHERE run_id=%s ORDER BY source_table',(run,)).fetchall()
    require(len(rows)==4 and all(a==b and equal and sha for t,a,b,equal,sha in rows),'FOUR_EXACT_STAGE_AUDITS_REQUIRED')
    from schema_contract import source_schema
    contract=source_schema();require({r[0] for r in rows}==set(contract),'AUDIT_TABLE_SCOPE')
    for table,a,b,equal,sha in rows:
        require(c.execute('SELECT hdg_load.stage_digest(%s,%s)',(contract[table]['mirror'],run)).fetchone()[0]==sha,'STAGE_CHANGED_AFTER_RECONCILE')
    return {t:{'read_rows':a,'staged_rows':b} for t,a,b,equal,sha in rows}

def publish(c,run):
    # BƯỚC 4: Publisher hiện có chỉ bootstrap/NOOP. Không tự dùng cutover-live hay chỉnh seal/checkpoint.
    with c.transaction():
        result=c.execute('SELECT hdg_load.publish_full_load(%s)',(run,)).fetchone()[0]
    counts=audits(c,run)
    return dict(status='PUBLISHED',outcome=result,tables={t:dict(v,copied_this_attempt=v['staged_rows'] if result=='COMMITTED' else 0) for t,v in counts.items()})

def execute(request,config):
    request=parameters(request);step=request['step']
    key=hashlib.sha256((request['target_db']+'|'+request['dag_run']).encode()).hexdigest()[:24]
    folder=ROOT/'test-results/airflow-etl'/key;folder.mkdir(parents=True,exist_ok=True)
    path=folder/'state.json';signature={k:request[k] for k in ('target_db','dag_run','source_scn','batch_size')};signature['oracle_url']=config['ORACLE_JDBC_URL']
    with target(config,request) as c:
        state=json.loads(path.read_text()) if path.exists() else dict(signature=signature,done={})
        require(state['signature']==signature,'RUN_PARAMETERS_CHANGED')
        index=STEPS.index(step)
        require(all(k in state['done'] for k in STEPS[:index]),'UPSTREAM_NOT_SUCCESS')
        source_identity(config)
        if step in state['done']:
            # File không thay thế DB guard. Run đã ghi phải giữ nguyên audit; kết quả cũ được báo là cached.
            if state.get('run_id'):audits(c,state['run_id'])
            return dict(step=step,status='REUSED_VERIFIED_RESULT',cached=True,processed_this_attempt=0,previous_result=state['done'][step])
        def save():
            temp=path.with_suffix('.tmp');temp.write_text(json.dumps(state,indent=2,default=str));temp.replace(path)
        started=time.monotonic()
        if step=='preflight':
            import pyspark
            require(pyspark.__version__=='4.0.1','SPARK_401_REQUIRED')
            for jar in ('ojdbc11.jar','postgresql-42.7.14.jar'):require((ROOT/'jobs/jars'/jar).is_file(),'JDBC_DRIVER_MISSING')
            require(c.execute('SELECT bootstrap_published FROM hdg_load.control WHERE id=1').fetchone()==(False,),'FRESH_BOOTSTRAP_CLONE_REQUIRED')
            from schema_contract import source_schema
            require(all(c.execute('SELECT count(*) FROM public.'+t['mirror']).fetchone()[0]==0 for t in source_schema().values()),'NONEMPTY_MIRROR_REQUIRES_MANUAL_CUTOVER')
            result=dict(status='READY',source_scn=request['source_scn'])
        elif step=='spark_stage':
            require(not state.get('spark_attempt_started'),'AMBIGUOUS_STAGE_ATTEMPT_USE_NEW_DAG_RUN')
            state['spark_attempt_started']=True;save()
            events=run_spark('full_load',['--stage-only','--source-scn',str(request['source_scn']),'--batch-size',str(request['batch_size'])],config,folder/f'spark-stage-{time.time_ns()}.log')
            complete=[e for e in events if e['event']=='run_staged'];require(len(complete)==1,'SPARK_COMPLETION_MISSING')
            run=complete[0]['run_id'];state['run_id']=run
            require(c.execute('SELECT status,source_scn FROM hdg_load.runs WHERE run_id=%s',(run,)).fetchone()==('VALIDATED',request['source_scn']),'STAGE_RUN_NOT_VALIDATED')
            result=dict(status='STAGED',run_id=run,tables=audits(c,run))
        elif step=='reconcile_stage':
            run=state['run_id'];events=run_spark('reconcile',['--run-id',run,'--target','stage','--source','snapshot','--record-stage-audit'],config,folder/f'spark-reconcile-stage-{time.time_ns()}.log')
            rows=[e for e in events if e['event']=='reconciliation'];require(len(rows)==4 and all(e['equal'] for e in rows),'STAGE_RECONCILE_FAILED')
            result=dict(status='VALIDATED',run_id=run,tables=audits(c,run))
        elif step=='publish_mirror':
            # Nếu commit đã xong nhưng mất phản hồi, không tự reset VALIDATED hay gọi publish mù.
            run=state['run_id'];require(c.execute('SELECT status FROM hdg_load.runs WHERE run_id=%s',(run,)).fetchone()==('VALIDATED',),'PUBLISH_OUTCOME_NEEDS_OPERATOR_REVIEW')
            result=publish(c,run)
        elif step=='c360_full':
            require(not state.get('c360_attempt_started'),'C360_OUTCOME_NEEDS_OPERATOR_REVIEW')
            state['c360_attempt_started']=True;save()
            from c360_full_load import full_load
            result=full_load(c)
            require(result['status'] in ('SUCCESS','SUCCESS_WITH_UNKNOWN'),'C360_FULL_FAILED')
        else:
            run=state['run_id'];events=run_spark('reconcile',['--run-id',run,'--target','mirror','--source','snapshot'],config,folder/f'spark-reconcile-mirror-{time.time_ns()}.log')
            require(len([e for e in events if e['event']=='reconciliation' and e['equal']])==4,'MIRROR_RECONCILE_FAILED')
            from c360_reconcile import reconcile
            result=reconcile(c);require(result['status'] in ('PASS','PASS_WITH_UNKNOWN'),'FINAL_C360_RECONCILE_FAILED')
        result=dict(result,step=step,elapsed_seconds=round(time.monotonic()-started,3),artifact_dir=str(folder))
        state['done'][step]=result;save();return result

if __name__=='__main__':
    try:
        request=parameters(json.load(sys.stdin));print(json.dumps(execute(request,configuration(request)),default=str))
    except Exception as error:
        print(json.dumps(dict(status='BLOCKED',error_type=type(error).__name__,reason=str(error) if isinstance(error,Blocked) else 'INSPECT_PRIVATE_RUNNER_LOG')));sys.exit(1)
