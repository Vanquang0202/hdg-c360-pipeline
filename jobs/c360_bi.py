"""Quản lý dashboard trên Superset HDG độc lập tại localhost:8089.
Đầu vào: HDG_BI_URI của tài khoản chỉ SELECT hdg_bi; HDG_BI_OWNER (mặc định HDG_ADMIN_USERNAME / hdg_admin).
Đầu ra: connection, dataset, chart và dashboard mới; không truy cập bảng nguồn để ghi.
Không cập nhật dashboard có sẵn: gặp trùng tên sẽ dừng để tránh ghi đè tùy chỉnh.
"""
import json
import os


def provision(app):
    assert_app(app.config)
    from superset import db
    from superset.models.core import Database
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice
    from superset.connectors.sqla.models import SqlaTable
    slug = 'hdg-c360-local'
    # BƯỚC 1: Kiểm tra đầu vào, chủ sở hữu và tên riêng trước khi tạo metadata BI.
    owner = app.appbuilder.sm.find_user(username=os.environ.get('HDG_BI_OWNER', os.environ.get('HDG_ADMIN_USERNAME', 'hdg_admin')))
    if not owner:
        raise ValueError('BI_OWNER_NOT_FOUND')
    if db.session.query(Dashboard).filter_by(slug=slug).first():
        raise ValueError('DASHBOARD_ALREADY_EXISTS: không ghi đè dashboard đã tạo')
    if db.session.query(Database).filter_by(database_name='HDG C360 read-only').first():
        raise ValueError('DATABASE_CONNECTION_ALREADY_EXISTS')
    uri = os.environ['HDG_BI_URI']
    if not uri.startswith('postgresql+psycopg2://hdg_bi_reader@') and not uri.startswith('postgresql+psycopg2://hdg_bi_reader:'):
        raise ValueError('REQUIRE_HDG_BI_READER')
    # BƯỚC 2: Kết nối chỉ đọc tới các view tổng hợp; mật khẩu được Superset mã hóa trong metadata.
    database = Database(database_name='HDG C360 read-only', sqlalchemy_uri=uri,
                        expose_in_sqllab=False, allow_dml=False, allow_ctas=False, allow_cvas=False)
    db.session.add(database);db.session.flush()
    views = ('reconcile_counts','population','identity_by_province','dq_flags',
             'quality_by_province','sync_batches','sync_minutes')
    datasets = {}
    for name in views:
        dataset = SqlaTable(table_name=name, schema='hdg_bi', database=database, owners=[owner])
        db.session.add(dataset);db.session.flush()
        dataset.fetch_metadata()
        datasets[name] = dataset
    # BƯỚC 3: Bảng số liệu giữ đủ cột; biểu đồ dùng tổng công dân đã chuẩn hóa, không đếm Mirror.
    charts=[]
    def add(title, view, group=None, metric=None):
        dataset=datasets[view]
        params=dict(datasource=str(dataset.id)+'__table', viz_type='table', query_mode='raw',
                    all_columns=[c.column_name for c in dataset.columns],row_limit=50000,
                    server_pagination=True,page_length=20,order_by_cols=[],adhoc_filters=[],
                    time_range='No filter')
        if group:
            params.update(viz_type='echarts_timeseries_bar',query_mode='aggregate',x_axis=group,
                          groupby=[],metrics=[dict(expressionType='SIMPLE',column=dict(column_name=metric),
                          aggregate='SUM',label=metric,optionName='metric_'+metric)],
                          x_axis_sort_asc=True,show_legend=False)
        chart=Slice(slice_name=title,datasource_id=dataset.id,datasource_type='table',
                    viz_type=params['viz_type'],params=json.dumps(params),owners=[owner],cache_timeout=0)
        db.session.add(chart);db.session.flush();charts.append(chart)
    add('Đối soát số dòng: nguồn / hợp lệ / công dân','reconcile_counts')
    add('1. Công dân theo tỉnh','identity_by_province','ten_tinh','citizens')
    add('1. Địa bàn, tuổi tròn, giới tính, IS_ALIVE','population')
    add('1. Tỷ lệ định danh 12 số theo tỉnh (%)','identity_by_province')
    add('2. Số công dân theo DQ_FLAGS','dq_flags','flag','citizens')
    add('2. Tỷ lệ từng cờ trên toàn bộ C360 (%)','dq_flags')
    add('2. Chất lượng theo tỉnh và UNKNOWN','quality_by_province')
    add('3. Lịch sử Full/NRT, gồm cả FAILED','sync_batches')
    add('3. Số lượt khóa xử lý mỗi phút (không phải distinct cả phút)','sync_minutes')
    add('3. Lượt MERGE mỗi phút','sync_minutes','batch_minute','merged_rows')
    # BƯỚC 4: Gắn chart vào layout, commit metadata một lần, giữ nguyên dashboard khác.
    position={'DASHBOARD_VERSION_KEY':'v2',
              'ROOT_ID':dict(type='ROOT',id='ROOT_ID',children=['GRID_ID']),
              'GRID_ID':dict(type='GRID',id='GRID_ID',children=[],parents=['ROOT_ID'])}
    for i,chart in enumerate(charts):
        rid='ROW-'+str(i);cid='CHART-'+str(chart.id)
        position['GRID_ID']['children'].append(rid)
        position[rid]=dict(type='ROW',id=rid,children=[cid],parents=['ROOT_ID','GRID_ID'],meta=dict(background='BACKGROUND_TRANSPARENT'))
        position[cid]=dict(type='CHART',id=cid,children=[],parents=['ROOT_ID','GRID_ID',rid],
                           meta=dict(chartId=chart.id,width=12,height=60,sliceName=chart.slice_name))
    dashboard=Dashboard(dashboard_title='HDG C360 — Nhân khẩu, chất lượng và đồng bộ',slug=slug,
                        owners=[owner],slices=charts,published=True,position_json=json.dumps(position),
                        json_metadata=json.dumps(dict(refresh_frequency=60)),
                        description='Địa bàn thường trú; NULL là chưa xác định. Tuổi chỉ tính từ ngày chuẩn đủ 8 số. UNKNOWN không phải mất công dân. Số lượt khóa mỗi phút có thể lặp qua các lô. Đối soát COUNT trên BI không thay job đối soát toàn trường.')
    db.session.add(dashboard);db.session.commit()
    return dict(dashboard_id=dashboard.id,url='/superset/dashboard/'+slug+'/',
                datasets=len(datasets),charts=len(charts))


def dashboard_main():
    assert_runtime()
    from superset.app import create_app
    app=create_app()
    with app.app_context():
        try:
            print(json.dumps(provision(app),ensure_ascii=False))
        except Exception:
            from superset import db
            db.session.rollback()
            raise


def setup():
    # BƯỚC 1: Chỉ cài schema/role mới; tên đã tồn tại thì dừng, không ghi đè cấu hình cũ.
    from pathlib import Path
    from psycopg import sql
    from c360_full_load import connect
    password=os.environ['HDG_BI_PASSWORD']
    if len(password)<20:
        raise ValueError('HDG_BI_PASSWORD cần ít nhất 20 ký tự')
    with connect() as conn:
        with conn.transaction():
            if conn.execute("SELECT 1 FROM pg_namespace WHERE nspname='hdg_bi'").fetchone():
                raise ValueError('BI_SCHEMA_ALREADY_EXISTS')
            if conn.execute("SELECT 1 FROM pg_roles WHERE rolname='hdg_bi_reader'").fetchone():
                raise ValueError('BI_ROLE_ALREADY_EXISTS')
            ddl=(Path(__file__).resolve().parents[1]/'sql/postgres/07_c360_bi.sql').read_text(encoding='utf-8')
            conn.execute(ddl.replace('BEGIN;','').replace('COMMIT;',''))
            conn.execute(sql.SQL('CREATE ROLE hdg_bi_reader LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT').format(sql.Literal(password)))
            conn.execute('ALTER ROLE hdg_bi_reader SET default_transaction_read_only=on')
            conn.execute('GRANT CONNECT ON DATABASE hdg_mirror TO hdg_bi_reader')
            conn.execute('GRANT USAGE ON SCHEMA hdg_bi TO hdg_bi_reader')
            conn.execute('GRANT SELECT ON ALL TABLES IN SCHEMA hdg_bi TO hdg_bi_reader')
    print('BI_VIEWS_AND_READ_ONLY_ROLE_INSTALLED',flush=True)


CONTAINER = 'hdg-c360-superset'


def assert_url(url):
    from urllib.parse import urlparse
    p=urlparse(url)
    if (p.scheme!='http' or p.hostname not in ('localhost','127.0.0.1') or p.port!=8089
        or p.username or p.password or p.query or p.fragment or p.path not in ('','/')):
        raise ValueError('HDG_BI_URL_MUST_BE_LOCAL_8089')
    return url.rstrip('/')


def assert_app(config):
    from urllib.parse import urlparse
    p=urlparse(config.get('SQLALCHEMY_DATABASE_URI',''))
    if (config.get('HDG_BI_INSTANCE')!='hdg-c360' or config.get('APP_NAME')!='HDG C360'
        or p.hostname!='metadata' or p.path!='/hdg_superset' or p.username!='hdg_superset'):
        raise ValueError('NOT_HDG_SUPERSET_METADATA')


def assert_runtime():
    # Chặn trước create_app(): Superset có thể sync metadata ngay khi khởi tạo ứng dụng.
    if (os.environ.get('HDG_BI_INSTANCE')!='hdg-c360'
        or os.environ.get('SUPERSET_CONFIG_PATH')!='/app/hdg_superset_config.py'):
        raise ValueError('NOT_HDG_RUNTIME')
    import runpy
    assert_app(runpy.run_path('/app/hdg_superset_config.py'))


def assert_container(info):
    labels=info['Config'].get('Labels') or {}
    ports=info['NetworkSettings'].get('Ports') or {}
    if (info.get('Name')!='/'+CONTAINER or labels.get('com.docker.compose.project')!='hdg-c360-bi'
        or labels.get('com.docker.compose.service')!='superset'
        or ports.get('8088/tcp')!=[dict(HostIp='127.0.0.1',HostPort='8089')]):
        raise ValueError('NOT_HDG_CONTAINER_OR_PORT')
    if any('superset_source' in m['Source'].lower() for m in info.get('Mounts',[])):
        raise ValueError('DATAHUB_MOUNT_FORBIDDEN')
    env=dict(v.split('=',1) for v in info['Config'].get('Env',[]) if '=' in v)
    if (env.get('HDG_BI_INSTANCE')!='hdg-c360'
        or env.get('SUPERSET_CONFIG_PATH')!='/app/hdg_superset_config.py'):
        raise ValueError('HDG_INSTANCE_MARKER_REQUIRED')


def checked_container():
    import subprocess
    assert_url(os.environ.get('HDG_SUPERSET_URL','http://localhost:8089'))
    if os.environ.get('HDG_SUPERSET_CONTAINER',CONTAINER)!=CONTAINER:
        raise ValueError('HDG_CONTAINER_OVERRIDE_FORBIDDEN')
    info=json.loads(subprocess.check_output(['docker','inspect',CONTAINER]))[0]
    assert_container(info)


def container_command(mode, extra_env=()):
    # BƯỚC 1: Guard kiểm Compose/port/mount trước mọi thao tác metadata HDG.
    import subprocess
    from pathlib import Path
    checked_container()
    command=['docker','exec','-i']
    for name in extra_env:command+=['-e',name]
    command += [CONTAINER,'python','-c',
        "import sys; code=sys.stdin.read(); exec(compile(code,'c360_bi.py','exec'))",mode]
    subprocess.run(command,input=Path(__file__).read_text(encoding='utf-8'),
                   text=True,encoding='utf-8',check=True)


def publish():
    # BƯỚC 2: Chỉ truyền mật khẩu BI qua environment; không dùng tài khoản owner của Mirror.
    from urllib.parse import quote
    os.environ['HDG_BI_URI']='postgresql+psycopg2://hdg_bi_reader:'+quote(os.environ['HDG_BI_PASSWORD'],safe='')+'@host.docker.internal:15432/hdg_mirror'
    os.environ['HDG_BI_OWNER']=os.environ.get('HDG_ADMIN_USERNAME','hdg_admin')
    container_command('dashboard',('HDG_BI_URI','HDG_BI_OWNER'))


def bootstrap_admin():
    assert_runtime()
    from superset.app import create_app
    app=create_app()
    with app.app_context():
        assert_app(app.config)
        sm=app.appbuilder.sm
        username=os.environ['HDG_ADMIN_USERNAME']
        if sm.find_user(username=username):
            print('HDG_ADMIN_ALREADY_EXISTS: password unchanged');return
        user=sm.add_user(username,'HDG','Admin','hdg-admin@localhost.invalid',
                         sm.find_role('Admin'),os.environ['HDG_ADMIN_PASSWORD'])
        if not user:raise ValueError('HDG_ADMIN_CREATION_FAILED')
        print('HDG_ADMIN_CREATED: '+username)


def bootstrap():
    import subprocess
    checked_container()
    subprocess.run(['docker','exec',CONTAINER,'superset','db','upgrade'],check=True)
    container_command('bootstrap-container',('HDG_ADMIN_USERNAME','HDG_ADMIN_PASSWORD'))
    subprocess.run(['docker','exec',CONTAINER,'superset','init'],check=True)


def verify(output=None):
    checked_container()
    return verify_http(assert_url(os.environ.get('HDG_SUPERSET_URL','http://localhost:8089')),
                       os.environ['HDG_ADMIN_USERNAME'],os.environ['HDG_ADMIN_PASSWORD'],output)


def verify_http(base, username, password, output=None):
    # Cùng kiểm chứng dashboard/chart-data cho CLI và Airflow, chỉ cho phép instance HDG 8089.
    import urllib.request,urllib.parse,http.cookiejar
    from datetime import datetime,timezone
    from pathlib import Path
    if base not in ('http://localhost:8089','http://127.0.0.1:8089','http://host.docker.internal:8089'):
        raise ValueError('HDG_BI_URL_REQUIRED')
    client=urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    token=None
    def request(path,body=None,csrf=None):
        headers={}
        if token:headers['Authorization']='Bearer '+token
        if csrf:headers['X-CSRFToken']=csrf
        if body is not None:headers['Content-Type']='application/json'
        req=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers=headers)
        with client.open(req,timeout=120) as response:
            return json.load(response)
    login=request('/api/v1/security/login',dict(username=username,password=password,provider='db',refresh=True))
    token=login['access_token']
    csrf=request('/api/v1/security/csrf_token/')['result']
    # Đăng nhập form để kiểm tra chính URL dashboard, không nhầm trang login HTTP 200 thành thành công.
    form=urllib.parse.urlencode(dict(username=username,password=password,csrf_token=csrf)).encode()
    with client.open(urllib.request.Request(base+'/login/',data=form),timeout=60) as r:
        if '/login' in r.url:raise ValueError('FORM_LOGIN_FAILED')
    csrf=request('/api/v1/security/csrf_token/')['result']
    dashboards=request('/api/v1/dashboard/')['result']
    matches=[d for d in dashboards if d.get('slug')=='hdg-c360-local']
    if len(matches)!=1:raise ValueError('HDG_DASHBOARD_MISSING_OR_AMBIGUOUS')
    dashboard=matches[0]
    with client.open(base+'/superset/dashboard/hdg-c360-local/',timeout=60) as r:
        if '/login' in r.url or r.status!=200:raise ValueError('DASHBOARD_HTTP_FAILED')
    charts=request('/api/v1/dashboard/'+str(dashboard['id'])+'/charts')['result']
    evidence=dict(observed_at=datetime.now(timezone.utc).isoformat(),url=base,
                  dashboard_id=dashboard['id'],dashboard_http=200,charts=[])
    for item in charts:
        detail=request('/api/v1/chart/'+str(item['id']))['result']
        form=json.loads(detail['params'])
        query=dict(columns=form['all_columns'] if form['viz_type']=='table' else [form['x_axis']],
                   metrics=[] if form['viz_type']=='table' else form['metrics'],
                   row_limit=50000,filters=[],extras={})
        result=request('/api/v1/chart/data',dict(datasource=dict(id=detail['datasource_id'],type='table'),
                       queries=[query],form_data=form,result_format='json',result_type='full',force=True),csrf)
        records=result.get('result',[])
        if not records or any(r.get('error') for r in records):raise ValueError('CHART_DATA_FAILED')
        evidence['charts'].append(dict(id=item['id'],status=200,rows=[len(r.get('data',[])) for r in records]))
    if len(evidence['charts'])!=10:raise ValueError('EXPECTED_TEN_HDG_CHARTS')
    if output:
        path=Path(output);path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text(json.dumps(evidence,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(evidence,ensure_ascii=False))


def load_local():
    from pathlib import Path
    root=Path(__file__).resolve().parents[1]
    allowed={'HDG_BI_PASSWORD','HDG_ADMIN_USERNAME','HDG_ADMIN_PASSWORD','HDG_SUPERSET_URL','HDG_SUPERSET_CONTAINER'}
    for path in (root/'jobs/.env',root/'bi/.env'):
        if path.exists():
            for line in path.read_text(encoding='utf-8-sig').splitlines():
                if not line.strip() or line.lstrip().startswith('#') or '=' not in line:continue
                key,value=line.split('=',1)
                if key.strip() in allowed:os.environ.setdefault(key.strip(),value.strip())


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=['install-views','bootstrap','publish','verify','dashboard','bootstrap-container'])
    parser.add_argument('--output')
    args=parser.parse_args()
    if args.command not in ('dashboard','bootstrap-container'):load_local()
    if args.command=='install-views':setup()
    elif args.command=='bootstrap':bootstrap()
    elif args.command=='publish':publish()
    elif args.command=='verify':verify(args.output)
    elif args.command=='bootstrap-container':bootstrap_admin()
    else:dashboard_main()
