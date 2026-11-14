"""Diễn tập additive topic identity trên logical clone mới; không reset test cũ."""
import sys,os,json,copy,uuid,subprocess,base64,re
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'jobs'),str(ROOT/'cdc/sink')]
import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
from c360_full_load import connect
import cutover,engine
from kafka_reader import load_bindings,assert_topic,canonical_uuid,read_ranges
from protocol import Rejected,digest


def main():
 if '--run-clone' not in sys.argv:raise SystemExit('Use --run-clone; always creates a new isolated database')
 out=ROOT/'test-results/topic-identity';proof_file=out/'proof.json';genesis=out/'kraft-genesis.txt'
 passed=[]
 with connect() as live:
  original=live.execute("SELECT plan FROM hdg_load.cutovers WHERE state='ACTIVE'").fetchone()[0]
  source_cp=cutover.checkpoints(live,original['manifest']['epoch']);source_hash=cutover.table_hashes(live)
  resume='--resume-clone' in sys.argv
  db=sys.argv[sys.argv.index('--resume-clone')+1] if resume else 'hdg_cutover_clone_'+uuid.uuid4().hex[:8]+'_test'
  assert re.fullmatch(r'hdg_cutover_clone_[0-9a-f]{8}_test',db)
  if not resume:
    live.execute(sql.SQL('CREATE DATABASE {} OWNER hdg_user').format(sql.Identifier(db)))
    dump=subprocess.run(['docker','exec','hdg-c360-postgres','pg_dump','-U','hdg_user','-d','hdg_mirror','-Fc','-n','public','-n','c360','-n','sink_meta','-n','hdg_load','-n','hdg_stage','--exclude-table-data=hdg_stage.*'],capture_output=True,check=True).stdout
    (out/'clone-source.dump').write_bytes(dump)
    restore=subprocess.run(['docker','exec','-i','hdg-c360-postgres','pg_restore','-U','hdg_user','-d',db],input=dump,capture_output=True)
    if restore.returncode:
     error=restore.stderr.decode(errors='replace')
     if not ('schema "public" already exists' in error and 'errors ignored on restore: 1' in error and error.count('pg_restore: error:')==1):raise RuntimeError('Clone restore failed: '+error[-800:])
     print('Default public already exists; validating all restored data/schema before proceeding',flush=True)
  config=live.info.get_parameters();config.update(dbname=db,password=os.environ['PG_PASSWORD'])
 with psycopg.connect(**config,autocommit=True) as c:
  assert c.execute("SELECT plan FROM hdg_load.cutovers WHERE state='ACTIVE'").fetchone()[0]==original,'Only pristine restored clone can resume'
  assert cutover.table_hashes(c)==source_hash and cutover.checkpoints(c,original['manifest']['epoch'])==source_cp
  assert c.execute("SELECT to_regclass('sink_meta.topic_identity_bindings')").fetchone()[0] is None
  print('Pristine clone restore validated; starting identity rehearsal',flush=True)
  plan=copy.deepcopy(original);plan['purpose']='clone-rehearsal';plan['target']['database']=db
  plan['manifest']['mode']='captured-fixture-test';plan['manifest']['baseline']['assertion']='verified-test-baseline'
  # Khởi tạo seal riêng cho OID clone; không đổi bất kỳ checkpoint/seal nào của live.
  original_seal=c.execute('SELECT before_ownership FROM hdg_load.cutovers WHERE id=%s',(plan['id'],)).fetchone()[0]
  c.execute('UPDATE hdg_load.cutovers SET plan=%s,before_ownership=%s WHERE id=%s',(Jsonb(plan),Jsonb(cutover.ownership(c)),plan['id']))
  c.execute('UPDATE sink_meta.epochs SET manifest=%s,manifest_hash=%s WHERE epoch=%s',(Jsonb(plan['manifest']),engine.manifest_hash(plan['manifest']),plan['manifest']['epoch']))
  before_cp=cutover.checkpoints(c,plan['manifest']['epoch']);before_data=cutover.table_hashes(c)
  before_meta=c.execute('SELECT md5(row_to_json(t)::text) FROM hdg_load.cutovers t WHERE id=%s',(plan['id'],)).fetchone()[0]
  def unchanged():
   assert before_cp==cutover.checkpoints(c,plan['manifest']['epoch']) and before_data==cutover.table_hashes(c)
   assert before_meta==c.execute('SELECT md5(row_to_json(t)::text) FROM hdg_load.cutovers t WHERE id=%s',(plan['id'],)).fetchone()[0]
  with cutover.target(c,plan,db,'identity-maintenance'):
   proof=cutover.prove_topic_identities(c,plan,genesis);passed.append('origin_crc_full_prefix_and_bijection')
   first=next(iter(proof['topics']));spec=proof['topics'][first]
   try:assert_topic(first,spec['legacy_id'],spec['native_id'],proof['cluster_id']);raise AssertionError('Legacy admitted without binding')
   except Rejected:pass
   passed.append('legacy_without_proof_blocked')
   with c.transaction(force_rollback=True):cutover.bind_topic_identities(c,plan,proof)
   assert c.execute("SELECT to_regclass('sink_meta.topic_identity_bindings')").fetchone()[0] is None
   unchanged();passed.append('failed_install_rollback_atomic')
   cutover.bind_topic_identities(c,plan,proof);unchanged();passed.append('additive_binding_old_metadata_unchanged')
  bindings=load_bindings(c,plan['manifest']['epoch'])
  for t,spec in proof['topics'].items():assert_topic(t,spec['legacy_id'],spec['native_id'],proof['cluster_id'],bindings)
  passed.append('all_five_native_ids_accepted')
  raw=bytearray(base64.urlsafe_b64decode(spec['native_id']+'=='));raw[0]^=1
  altered=base64.urlsafe_b64encode(raw).decode().rstrip('=')
  try:assert_topic(t,spec['legacy_id'],altered,proof['cluster_id'],bindings);raise AssertionError('Lost upper bits ignored')
  except Rejected:pass
  passed.append('same_lower_bits_different_native_blocked')
  try:assert_topic(t,spec['legacy_id'],spec['native_id'],'different-cluster',bindings);raise AssertionError('Wrong cluster admitted')
  except Rejected:pass
  passed.append('different_cluster_blocked')
  with c.transaction(force_rollback=True):
   c.execute('UPDATE sink_meta.topic_identity_bindings SET native_id=%s WHERE epoch=%s AND topic=%s',(altered,proof['epoch'],t))
   try:load_bindings(c,proof['epoch']);raise AssertionError('Tampered binding admitted')
   except Rejected:pass
  passed.append('tampered_binding_proof_blocked')
  with cutover.target(c,plan,db,'identity-maintenance'):cutover.bind_topic_identities(c,plan,proof,rollback=True)
  try:load_bindings(c,proof['epoch']);raise AssertionError('Revoked binding admitted')
  except Rejected:pass
  unchanged();passed.append('revoke_fail_closed_no_data_or_offset_rewind')
  with cutover.target(c,plan,db,'identity-maintenance'):cutover.bind_topic_identities(c,plan,proof)
  with cutover.target(c,plan,db,'replay'):
   outcomes=cutover.replay(c,plan)
   assert all(x=='REPLAY_FRAME' for x in outcomes)
  unchanged();passed.append('reactivation_historical_replay_fingerprints_unchanged')
  assert c.execute('SELECT count(*) FROM sink_meta.quarantine').fetchone()[0]==0
  result=dict(status='PASS',database=db,tests=passed,passed=len(passed),records=proof['wire_records'],checkpoint_unchanged=True,seal_manifest_unchanged=True,clone_setup_source_ownership=original_seal)
  (out/'clone-result.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
 with connect() as live:
  assert source_cp==cutover.checkpoints(live,original['manifest']['epoch']) and source_hash==cutover.table_hashes(live)
 print(json.dumps({k:v for k,v in result.items() if k!='clone_setup_source_ownership'}))

if __name__=='__main__':main()

