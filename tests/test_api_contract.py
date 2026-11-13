"""Migration contract: Flask reference artifact ↔ Spring Boot qua HTTP trên DB test mới.
Không phục vụ API Python; sau migration cần --reference-dir trỏ archive local, không commit archive.
"""
import argparse,importlib.util,sys,os,json,subprocess,threading,time,urllib.request,urllib.error,urllib.parse,secrets,socket,hashlib,uuid
from pathlib import Path
from datetime import datetime,timezone,timedelta
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'jobs'))


def module(name,path):
    spec=importlib.util.spec_from_file_location(name,path);obj=importlib.util.module_from_spec(spec);sys.modules[name]=obj;spec.loader.exec_module(obj);return obj


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--reference-dir',type=Path,default=ROOT);args=parser.parse_args()
    ref=args.reference_dir
    if not (ref/'api/app.py').exists() or not (ref/'tests/test_api.py').exists():
        raise SystemExit('BLOCKED: provide frozen Flask reference artifact with --reference-dir')
    python=module('app',ref/'api/app.py');legacy=module('legacy_api_test',ref/'tests/test_api.py')
    legacy.__file__=str(ROOT/'tests/test_api.py') # Chỉ dùng fixture code cũ, tài nguyên DDL/.env lấy tại repo hiện tại.
    fixture=legacy.DatabaseAPI;fixture.setUpClass()
    run_id=uuid.uuid4().hex[:8];out=ROOT/'test-results/c360-api'/('java-migration-'+run_id);out.mkdir(parents=True)
    secret=secrets.token_urlsafe(48);env=dict(os.environ,C360_API_PG_PORT='15433',C360_API_DATABASE=fixture.db,
      C360_API_DB_USER=fixture.user,C360_API_DB_PASSWORD=fixture.password,C360_API_CURSOR_SECRET=secret)
    from werkzeug.serving import make_server,WSGIRequestHandler
    class Quiet(WSGIRequestHandler):
        def log_request(self,*a,**k):pass
    server=make_server('127.0.0.1',18091,python.create_app(fixture.connect,fixture.audit,secret),request_handler=Quiet)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    jar=ROOT/'api/target/c360-api-1.0.0.jar';process=None;log=None
    def start(delay='100'):
        nonlocal process,log
        log=(out/('java-'+delay+'.log')).open('a',encoding='utf-8')
        process=subprocess.Popen(['java','-Xms64m','-Xmx256m','-jar',str(jar),'--server.port=18090',
          '--c360.audit.path='+str(out/'outbox'),'--c360.audit.delay='+delay,'--c360.audit.initial-delay='+(delay if delay!='100' else '0')],cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT,
          creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        for _ in range(120):
            if process.poll() is not None:raise RuntimeError('Java exited; inspect local log')
            try:
                status,_=call(18090,'/api/v1/ca-nhan/001',key=fixture.key)
                if status==200:return
            except (OSError,urllib.error.URLError):pass
            time.sleep(.5)
        raise RuntimeError('Java startup timeout')
    def stop():
        nonlocal process,log
        if process and process.poll() is None:process.kill();process.wait(timeout=15)
        if log:log.close()
    def call(port,path,body=None,key=None,raw=False):
        headers={}
        if key:headers['X-API-Key']=key
        data=None
        if body is not None:
            headers['Content-Type']='application/json';data=body.encode() if raw else json.dumps(body,ensure_ascii=False).encode()
        req=urllib.request.Request('http://127.0.0.1:'+str(port)+path,data=data,headers=headers)
        try:r=urllib.request.urlopen(req,timeout=30)
        except urllib.error.HTTPError as e:r=e
        return r.status,json.load(r)
    cases=[]
    def compare(name,path,body=None,key='normal',raw=False):
        actual_key=fixture.key if key=='normal' else fixture.restricted if key=='restricted' else key
        a=call(18091,path,body,actual_key,raw);b=call(18090,path,body,actual_key,raw)
        ok=a==b;cases.append(dict(case=name,equal=ok,python_status=a[0],java_status=b[0]))
        if not ok:raise AssertionError('Contract mismatch: '+name+' (responses contain synthetic fixtures; inspect in debugger)')
        return a,b
    try:
        start()
        for name,code in [('current-priority','001'),('old','OLD-UNIQUE'),('identity','001000000001'),('missing','NO-MATCH'),('duplicate-old','OLD-DUP'),('duplicate-identity','001000000009'),('bad-code','A'*31),("sql-literal","x' OR 1=1--"),('unicode-code','ĐẶC-THÙ'),('plus-code','x+y')]:
            compare(name,'/api/v1/ca-nhan/'+urllib.parse.quote(code,safe=''))
        for name,key in [('missing-key',None),('wrong-key',secrets.token_urlsafe(32))]:compare(name,'/api/v1/ca-nhan/001',key=key)
        compare('scope','/api/v1/ca-nhan/thay-doi',key='restricted')
        compare('encoded-static-scope','/api/v1/ca-nhan/%74im-kiem',key='restricted')
        compare('matrix-code-not-truncated','/api/v1/ca-nhan/001;v=1')
        compare('matrix-static-is-code','/api/v1/ca-nhan/tim-kiem;v=1')
        compare('mixed-batch','/api/v1/ca-nhan/tra-cuu-lo',{'codes':['001','NO-MATCH','OLD-DUP','001']})
        compare('maximum-batch','/api/v1/ca-nhan/tra-cuu-lo',{'codes':['001']*1000})
        for i,body in enumerate([{},[],{'codes':[]},{'codes':['001']*1001},{'codes':[12]},{'codes':[' A']},{'codes':['001'],'extra':1}]):compare('invalid-batch-'+str(i),'/api/v1/ca-nhan/tra-cuu-lo',body)
        compare('malformed-json','/api/v1/ca-nhan/tra-cuu-lo','{bad',raw=True)
        for i,(name,dob) in enumerate([('Nguyễn Văn An','19950815'),('nguyen van an','19950815'),(' Nguyễn  Văn An ','1995'),('Nguyễn\u00a0Văn An','199508'),('Other','1995'),('An','19950230'),('','1995'),('An','19950000'),('An','0000'),('An','20000229')]):
            compare('search-'+str(i),'/api/v1/ca-nhan/tim-kiem?'+urllib.parse.urlencode({'hoTen':name,'ngaySinh':dob}))
        compare('search-truncated','/api/v1/ca-nhan/tim-kiem?'+urllib.parse.urlencode({'hoTen':'Nguyễn Văn An','ngaySinh':'1995','limit':'1'}))
        for q in ['cursor=bad','limit=0','limit=1001','limit=','limit=-1','limit=1.0']:
            compare('bad-page-'+q,'/api/v1/ca-nhan/thay-doi?'+q)
        # Cùng watermark và HMAC secret: so JSON chính xác, bao gồm cursor và timestamp tới microsecond.
        until=(datetime.now(timezone.utc)-timedelta(seconds=121)).isoformat()
        c=python.encode_cursor(dict(date='0001-01-01T00:00:00+00:00',id=0,until=until,client='api-test'),secret)
        pages=0
        while c:
            a,b=compare('shared-cursor-page-'+str(pages),'/api/v1/ca-nhan/thay-doi?limit=2&cursor='+urllib.parse.quote(c,safe=''))
            c=b[1]['nextCursor'];pages+=1
        a=call(18091,'/api/v1/ca-nhan/thay-doi?limit=2',key=fixture.key)
        b=call(18090,'/api/v1/ca-nhan/thay-doi?limit=2',key=fixture.key)
        assert a[0]==b[0]==200 and a[1]['items']==b[1]['items'] and a[1]['limitations']==b[1]['limitations']
        compare('python-cursor-to-java','/api/v1/ca-nhan/thay-doi?limit=2&cursor='+urllib.parse.quote(a[1]['nextCursor'],safe=''))
        compare('java-cursor-to-python','/api/v1/ca-nhan/thay-doi?limit=2&cursor='+urllib.parse.quote(b[1]['nextCursor'],safe=''))
        # Dừng/khởi động process Java thật với batch chưa flush: pending phải được giữ và replay idempotent.
        stop();start('3600000')
        call(18090,'/api/v1/ca-nhan/001',key=fixture.key)
        pending=list((out/'outbox').glob('*.json'));assert pending
        copied=[(p.name,p.read_bytes()) for p in pending];ids=[json.loads(data)['requestId'] for _,data in copied]
        stop();start('100')
        deadline=time.monotonic()+10
        while list((out/'outbox').glob('*.json')) and time.monotonic()<deadline:time.sleep(.1)
        with fixture.owner(fixture.db) as conn:
            assert conn.execute('SELECT count(*) FROM c360_api.c360_api_audit WHERE request_id=ANY(%s::uuid[])',(ids,)).fetchone()[0]==len(ids)
        stop()
        for name,data in copied:(out/'outbox'/name).write_bytes(data)
        start('100');time.sleep(.5)
        with fixture.owner(fixture.db) as conn:
            assert conn.execute('SELECT count(*) FROM c360_api.c360_api_audit WHERE request_id=ANY(%s::uuid[])',(ids,)).fetchone()[0]==len(ids)
            assert conn.execute('SELECT count(*) FROM c360.c360_thong_tin_ca_nhan').fetchone()[0]==7
        result=dict(database=fixture.db,contract_cases=cases,exact_matches=len(cases),cursor_pages=pages,
                    real_process_restart='PASS',audit_replay_idempotent='PASS',business_rows=7)
        (out/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
        print(json.dumps(dict(evidence=str(out),database=fixture.db,exact_matches=len(cases),restart='PASS',replay='PASS')))
    finally:
        stop();server.shutdown();thread.join(timeout=5);fixture.audit.close()
        if not (out/'result.json').exists():(out/'failed-cases.json').write_text(json.dumps(cases,indent=2),encoding='utf-8')

if __name__=='__main__':main()
