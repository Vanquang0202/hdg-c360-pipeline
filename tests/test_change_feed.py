"""Journal C360 trên DB test mới; không reset clone/metadata cũ. --run-clone mới ghi test."""
import os,sys,json,uuid,time,threading,subprocess,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'cdc/sink'))
import psycopg
from psycopg import sql
from storage import password

@unittest.skipUnless('--run-clone' in sys.argv,'Requires --run-clone isolated PostgreSQL')
class ChangeFeedTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.db='hdg_api_'+uuid.uuid4().hex[:8]+'_test'
  cls.config=dict(host='127.0.0.1',port=15433,user='sink_test_owner',password=password(),autocommit=True)
  with psycopg.connect(dbname='postgres',**cls.config) as c:
   for name in ('hdg_c360_api','hdg_airflow_reader'):
    if not c.execute('SELECT 1 FROM pg_roles WHERE rolname=%s',(name,)).fetchone():c.execute(sql.SQL('CREATE ROLE {} NOLOGIN').format(sql.Identifier(name)))
   c.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(cls.db)))
  with cls.connect() as c:
   c.execute((ROOT/'sql/postgres/05_c360.sql').read_text(encoding='utf-8'))
   cls.insert(c,'BASELINE')
   c.execute((ROOT/'sql/postgres/08_c360_api.sql').read_text(encoding='utf-8'))
   c.execute((ROOT/'sql/postgres/09_c360_change_feed.sql').read_text(encoding='utf-8'))
  out=ROOT/'test-results/change-feed';out.mkdir(exist_ok=True)
  (out/'clone.json').write_text(json.dumps(dict(database=cls.db,port=15433)),encoding='utf-8')
 @classmethod
 def connect(cls):return psycopg.connect(dbname=cls.db,**cls.config)
 @staticmethod
 def insert(c,code):
  c.execute("INSERT INTO c360.c360_thong_tin_ca_nhan(ma_so_bhxh,ho_ten,dq_flags,unknown_rules,normalization_status,source_row_goc,run_id,sync_date) VALUES(%s,'Clone fixture','', '{}'::jsonb,'COMPLETE','{}'::jsonb,%s,clock_timestamp())",(code,uuid.uuid4()))
 def test_bootstrap(self):
  with self.connect() as c:self.assertEqual(c.execute("SELECT operation,payload->>'maSoBhxh' FROM c360.change_events WHERE event_id=1").fetchone(),('UPSERT','BASELINE'))
 def test_insert_update_delete_retry(self):
  code='IUD-'+uuid.uuid4().hex[:8]
  with self.connect() as c:
   self.insert(c,code)
   c.execute('UPDATE c360.c360_thong_tin_ca_nhan SET ho_ten=ho_ten,sync_date=clock_timestamp() WHERE ma_so_bhxh=%s',(code,))
   c.execute("UPDATE c360.c360_thong_tin_ca_nhan SET ho_ten='Changed' WHERE ma_so_bhxh=%s",(code,))
   c.execute('DELETE FROM c360.c360_thong_tin_ca_nhan WHERE ma_so_bhxh=%s',(code,))
   rows=c.execute('SELECT operation,payload FROM c360.change_events WHERE ma_so_bhxh=%s ORDER BY event_id',(code,)).fetchall()
   self.assertEqual([r[0] for r in rows],['UPSERT','UPSERT','DELETE']);self.assertEqual(rows[-1][1],dict(maSoBhxh=code,maSoBhxhCu=None))
 def test_rollback_does_not_allocate_visible_id(self):
  with self.connect() as c:
   high=c.execute('SELECT last_id FROM c360.change_feed_control').fetchone()[0]
   with c.transaction(force_rollback=True):self.insert(c,'ROLLBACK')
   self.assertEqual(c.execute('SELECT last_id FROM c360.change_feed_control').fetchone()[0],high)
   self.assertEqual(c.execute("SELECT count(*) FROM c360.change_events WHERE ma_so_bhxh='ROLLBACK'").fetchone()[0],0)
 def test_concurrent_late_commit_is_serial(self):
  first=self.connect();second=self.connect();done=threading.Event();errors=[]
  code='LATE-'+uuid.uuid4().hex[:6];other=code+'B'
  first.execute('BEGIN');self.insert(first,code)
  pid=second.execute('SELECT pg_backend_pid()').fetchone()[0]
  def write():
   try:self.insert(second,other);done.set()
   except Exception as e:errors.append(type(e).__name__)
  t=threading.Thread(target=write);t.start()
  try:
   with self.connect() as observer:
    deadline=time.monotonic()+5
    while time.monotonic()<deadline:
     if observer.execute('SELECT cardinality(pg_blocking_pids(%s))',(pid,)).fetchone()[0]:break
     time.sleep(.05)
    else:self.fail('Second writer did not wait for transaction fence')
    self.assertFalse(done.is_set());self.assertEqual(observer.execute('SELECT count(*) FROM c360.change_events WHERE ma_so_bhxh=ANY(%s)',([code,other],)).fetchone()[0],0)
   first.execute('COMMIT');t.join(10);self.assertTrue(done.is_set());self.assertFalse(errors)
   with self.connect() as c:self.assertEqual([r[0] for r in c.execute('SELECT ma_so_bhxh FROM c360.change_events WHERE ma_so_bhxh=ANY(%s) ORDER BY event_id',([code,other],)).fetchall()],[code,other])
  finally:
   if first.info.transaction_status!=psycopg.pq.TransactionStatus.IDLE:first.execute('ROLLBACK')
   t.join(10);first.close();second.close()
 def test_hard_crash_before_after_commit(self):
  for when in ('before','after'):
   code='CRASH-'+when+'-'+uuid.uuid4().hex[:6]
   program="import os,sys;sys.path.insert(0,'tests');from test_change_feed import ChangeFeedTests as T;T.db=os.environ['FEED_TEST_DB'];T.config=dict(host='127.0.0.1',port=15433,user='sink_test_owner',password=os.environ['CDC_TEST_PASSWORD'],autocommit=False);c=T.connect();T.insert(c,os.environ['FEED_TEST_CODE']);"+('c.commit();' if when=='after' else '')+'os._exit(91)'
   child=subprocess.run([sys.executable,'-c',program],cwd=ROOT,env=dict(os.environ,FEED_TEST_DB=self.db,FEED_TEST_CODE=code,CDC_TEST_PASSWORD=password()),timeout=20)
   self.assertEqual(child.returncode,91)
   with self.connect() as c:self.assertEqual(c.execute('SELECT count(*) FROM c360.change_events WHERE ma_so_bhxh=%s',(code,)).fetchone()[0],1 if when=='after' else 0)
 def test_repeatable_read_retries_whole_transaction(self):
  code='RR-'+uuid.uuid4().hex[:8]
  with self.connect() as first,self.connect() as second:
   first.execute('BEGIN');self.insert(first,code+'A')
   second.execute('BEGIN ISOLATION LEVEL REPEATABLE READ')
   second.execute('SELECT last_id FROM c360.change_feed_control').fetchone()
   first.execute('COMMIT')
   with self.assertRaises(psycopg.errors.SerializationFailure):self.insert(second,code+'B')
   second.execute('ROLLBACK');self.insert(second,code+'B')
   self.assertEqual(second.execute('SELECT count(*) FROM c360.change_events WHERE ma_so_bhxh=%s',(code+'B',)).fetchone()[0],1)
 def test_truncate_is_blocked(self):
  with self.connect() as c:
   with self.assertRaises(psycopg.errors.RaiseException):c.execute('TRUNCATE c360.c360_thong_tin_ca_nhan')
 def test_z_retention_prefix(self):
  with self.connect() as c:
   c.execute("UPDATE c360.change_events SET occurred_at=clock_timestamp()-interval '40 days'")
   high=c.execute('SELECT last_id FROM c360.change_feed_control').fetchone()[0]
   removed=c.execute('SELECT c360.prune_change_events(30)').fetchone()[0]
   self.assertGreater(removed,0);self.assertEqual(c.execute('SELECT retained_after FROM c360.change_feed_control').fetchone()[0],high)
   self.insert(c,'AFTER_RETENTION');self.assertEqual(c.execute('SELECT min(event_id) FROM c360.change_events').fetchone()[0],high+1)

if __name__=='__main__':
 argv=[v for v in sys.argv if v!='--run-clone']
 unittest.main(argv=argv,verbosity=2)
